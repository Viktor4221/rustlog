pub mod cache;

use self::cache::UsersCache;
use crate::{
    config::Config,
    db::{delete_user_logs, writer::FlushBuffer},
    error::Error,
    Result,
};
use anyhow::Context;
use chrono::Utc;
use dashmap::DashMap;
use dashmap::DashSet;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, error, info};
use twitch_api::{helix::users::GetUsersRequest, twitch_oauth2::AppAccessToken, HelixClient};

#[derive(Clone)]
pub struct App {
    pub helix_client: HelixClient<'static, reqwest::Client>,
    pub token: Arc<AppAccessToken>,
    pub users: UsersCache,
    pub optout_codes: Arc<DashSet<String>>,
    pub db: Arc<clickhouse::Client>,
    pub config: Arc<Config>,
    pub flush_buffer: FlushBuffer,
    pub pg: Arc<PgPool>,
    /// In-memory map of user_id -> current username for change detection
    pub username_cache: Arc<DashMap<String, String>>,
}

impl App {
    /// Called on every chat message. Checks if the username has changed since we
    /// last saw this user and, if so, upserts the `usernames` table and appends a
    /// row to `username_history`. All Postgres errors are logged but never
    /// propagated — a DB hiccup must never affect the main logging pipeline.
    pub async fn track_username(&self, user_id: &str, username: &str) {
        // Fast path: username unchanged since last message — nothing to do.
        if let Some(cached) = self.username_cache.get(user_id) {
            if cached.value() == username {
                return;
            }
        }

        // Either first time we see this user, or username changed.
        // Upsert into `usernames` and capture the previous value (if any).
        let result: Result<Option<String>, _> = sqlx::query_scalar(
            r#"
            INSERT INTO usernames (user_id, username)
            VALUES ($1::int4, $2)
            ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username
                WHERE usernames.username IS DISTINCT FROM EXCLUDED.username
            RETURNING (SELECT username FROM usernames WHERE user_id = $1::int4)
            "#,
        )
        .bind(user_id.parse::<i32>().unwrap_or(0))
        .bind(username)
        .fetch_optional(&*self.pg)
        .await;

        match result {
            Err(e) => {
                error!("Postgres upsert failed for user {user_id}: {e}");
                return;
            }
            Ok(maybe_old) => {
                // `maybe_old` is Some(old_username) when the UPDATE branch fired
                // (i.e. the username actually changed), None on a fresh INSERT.
                if let Some(old_username) = maybe_old {
                    let ts = Utc::now().timestamp_millis();
                    if let Err(e) = sqlx::query(
                        r#"
                        INSERT INTO username_history (user_id, ts, old_username, new_username)
                        VALUES ($1::int4, $2, $3, $4)
                        "#,
                    )
                    .bind(user_id.parse::<i32>().unwrap_or(0))
                    .bind(ts)
                    .bind(&old_username)
                    .bind(username)
                    .execute(&*self.pg)
                    .await
                    {
                        error!("Postgres history insert failed for user {user_id}: {e}");
                    } else {
                        info!(
                            "Username change detected: {user_id} {old_username} -> {username}"
                        );
                    }
                }
            }
        }

        // Update the in-memory cache regardless of whether it was an INSERT or UPDATE.
        self.username_cache
            .insert(user_id.to_owned(), username.to_owned());
    }

    pub async fn get_users(
        &self,
        ids: Vec<String>,
        names: Vec<String>,
        ignore_cache: bool,
    ) -> Result<HashMap<String, String>> {
        let mut users = HashMap::new();
        let mut ids_to_request = Vec::new();
        let mut names_to_request = Vec::new();

        if ignore_cache {
            ids_to_request.clone_from(&ids);
            names_to_request.clone_from(&names);
        } else {
            for id in ids {
                match self.users.get_login(&id) {
                    Some(Some(login)) => {
                        users.insert(id, login);
                    }
                    Some(None) => (),
                    None => ids_to_request.push(id),
                }
            }

            for name in names {
                match self.users.get_id(&name) {
                    Some(Some(id)) => {
                        users.insert(id, name);
                    }
                    Some(None) => (),
                    None => names_to_request.push(name),
                }
            }
        }

        let mut new_users = Vec::with_capacity(ids_to_request.len() + names_to_request.len());

        // There are no chunks if the vec is empty, so there is no empty request made
        for chunk in ids_to_request.chunks(100) {
            debug!("Requesting user info for ids {chunk:?}");

            let request = GetUsersRequest::ids(chunk);
            let response = self.helix_client.req_get(request, &*self.token).await?;
            new_users.extend(response.data);
        }

        for chunk in names_to_request.chunks(100) {
            debug!("Requesting user info for names {chunk:?}");

            let request = GetUsersRequest::logins(chunk);
            let response = self.helix_client.req_get(request, &*self.token).await?;
            new_users.extend(response.data);
        }

        for user in new_users {
            let id = user.id.to_string();
            let login = user.login.to_string();

            self.users.insert(id.clone(), login.clone());

            users.insert(id, login);
        }

        // Banned users which were not returned by the api
        for id in ids_to_request {
            if !users.contains_key(id.as_str()) {
                self.users.insert_optional(Some(id), None);
            }
        }
        for name in names_to_request {
            if !users.values().any(|login| login == name.as_str()) {
                self.users.insert_optional(None, Some(name));
            }
        }

        Ok(users)
    }

    pub async fn get_user_id_by_name(&self, name: &str) -> Result<String> {
        match self.users.get_id(name) {
            Some(Some(id)) => Ok(id),
            Some(None) => Err(Error::NotFound),
            None => {
                let request = GetUsersRequest::logins(vec![name]);
                let response = self.helix_client.req_get(request, &*self.token).await?;
                match response.data.into_iter().next() {
                    Some(user) => {
                        let user_id = user.id.to_string();
                        self.users.insert(user_id.clone(), user.login.to_string());
                        Ok(user_id)
                    }
                    None => {
                        self.users.insert_optional(None, Some(name.to_owned()));
                        Err(Error::NotFound)
                    }
                }
            }
        }
    }

    pub async fn optout_user(&self, user_id: &str) -> anyhow::Result<()> {
        delete_user_logs(&self.db, user_id)
            .await
            .context("Could not delete logs")?;

        self.config.opt_out.insert(user_id.to_owned(), true);
        self.config.save()?;
        info!("User {user_id} opted out");

        Ok(())
    }

    pub fn check_opted_out(&self, channel_id: &str, user_id: Option<&str>) -> Result<()> {
        if self.config.opt_out.contains_key(channel_id) {
            return Err(Error::ChannelOptedOut);
        }

        if let Some(user_id) = user_id {
            if self.config.opt_out.contains_key(user_id) {
                return Err(Error::UserOptedOut);
            }
        }

        Ok(())
    }
}
