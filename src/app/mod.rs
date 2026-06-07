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

        let uid: i64 = match user_id.parse() {
            Ok(v) => v,
            Err(e) => {
                error!("Could not parse user_id {user_id:?} as integer: {e}");
                return;
            }
        };

        // Atomically claim this update in the in-memory cache before touching
        // Postgres. DashMap::insert returns the previous value. If another
        // concurrent task already raced ahead and inserted the same username,
        // the returned old value will equal `username` — bail out so only one
        // task performs the DB writes.
        // This also ensures the cache is always updated even when the DB call
        // below fails, preventing log spam on every subsequent message.
        let previous_cached = self
            .username_cache
            .insert(user_id.to_owned(), username.to_owned());
        if previous_cached.as_deref() == Some(username) {
            // Another concurrent call already handled this transition.
            return;
        }

        // Capture the old username *before* overwriting it, then do the upsert.
        // The CTE `old` snapshots the current row; the UPDATE only fires when the
        // username actually differs; RETURNING gives us the pre-update value so we
        // can tell whether a real change happened.
        //
        // Row is returned when:
        //   - INSERT (new user)  -> old_username is NULL
        //   - UPDATE (changed)   -> old_username is the previous value
        // No row is returned when the ON CONFLICT ... WHERE clause is false
        // (username identical) because postgres skips the UPDATE entirely.
        let result: std::result::Result<Option<Option<String>>, sqlx::Error> = sqlx::query_scalar(
            r#"
            WITH old AS (
                SELECT username FROM usernames WHERE user_id = $1::int8
            )
            INSERT INTO usernames (user_id, username)
            VALUES ($1::int8, $2)
            ON CONFLICT (user_id) DO UPDATE
                SET username = EXCLUDED.username
                WHERE usernames.username IS DISTINCT FROM EXCLUDED.username
            RETURNING (SELECT username FROM old)
            "#,
        )
        .bind(uid)
        .bind(username)
        .fetch_optional(&*self.pg)
        .await;

        match result {
            Err(e) => {
                error!("Postgres upsert failed for user {user_id}: {e}");
                // The in-memory cache was already updated above. We intentionally
                // do not revert it: the next username *change* will trigger a
                // fresh DB attempt, while repeated messages with the same username
                // are silently skipped. This avoids hammering a broken DB on every
                // single message from a busy user.
            }
            // No row returned = username was identical in DB, nothing changed.
            Ok(None) => {}
            // Row returned = INSERT (old_username is None) or UPDATE (old_username is Some).
            Ok(Some(maybe_old_username)) => {
                if let Some(old_username) = maybe_old_username {
                    // Username changed — write history record.
                    let ts = Utc::now().timestamp_millis();
                    if let Err(e) = sqlx::query(
                        r#"
                        INSERT INTO username_history (user_id, ts, old_username, new_username)
                        VALUES ($1::int8, $2::int8, $3, $4)
                        "#,
                    )
                    .bind(uid)
                    .bind(ts)
                    .bind(&old_username)
                    .bind(username)
                    .execute(&*self.pg)
                    .await
                    {
                        error!("Postgres history insert failed for user {user_id}: {e}");
                    } else {
                        info!("Username change: {user_id} {old_username} -> {username}");
                    }
                }
                // Fresh INSERT (maybe_old_username was None): no history row needed.
            }
        }
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
