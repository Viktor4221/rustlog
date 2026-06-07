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
    /// In-memory map of user_id -> current username for change detection.
    /// Pre-seeded from the `usernames` table at startup; kept up-to-date on
    /// every successful DB write so that restarts never cause renames to be
    /// silently dropped.
    pub username_cache: Arc<DashMap<String, String>>,
}

impl App {
    /// Pre-seed `username_cache` from the `usernames` Postgres table.
    /// Call once at startup, before the bot begins processing messages.
    pub async fn seed_username_cache(&self) {
        match sqlx::query_as::<_, (i32, String)>("SELECT user_id, username FROM usernames")
            .fetch_all(&*self.pg)
            .await
        {
            Ok(rows) => {
                let count = rows.len();
                for (user_id, username) in rows {
                    self.username_cache.insert(user_id.to_string(), username);
                }
                info!("Seeded username_cache with {count} entries from Postgres");
            }
            Err(e) => {
                error!("Could not seed username_cache from Postgres: {e}");
            }
        }
    }

    /// Called on every chat message. Checks if the username has changed since
    /// we last saw this user and, if so, upserts the `usernames` table and
    /// appends a row to `username_history`. All Postgres errors are logged but
    /// never propagated — a DB hiccup must never affect the main logging
    /// pipeline.
    pub async fn track_username(&self, user_id: &str, username: &str) {
        // Fast path: username unchanged since last message — nothing to do.
        if let Some(cached) = self.username_cache.get(user_id) {
            if cached.value() == username {
                return;
            }
        }

        let uid: i32 = match user_id.parse() {
            Ok(v) => v,
            Err(e) => {
                error!("Could not parse user_id {user_id:?} as integer: {e}");
                return;
            }
        };

        // Upsert the `usernames` table, capturing the old username atomically
        // in a CTE so no separate round-trip is needed and no rename can be
        // lost even after a restart.
        //
        // The CTE reads the existing row *before* the INSERT/UPDATE runs.
        // RETURNING then gives us:
        //   inserted    = true  → fresh INSERT (new user, no history row needed)
        //   inserted    = false → UPDATE (name changed, write history row)
        //   old_username        → the name that was in the table before this
        //                         upsert; NULL when the row did not exist yet
        //
        // The WHERE clause on DO UPDATE prevents any write — and therefore any
        // RETURNING row — when the username is already identical in the DB.
        let result: std::result::Result<Option<(bool, Option<String>)>, sqlx::Error> =
            sqlx::query_as(
                r#"
                WITH old AS (
                    SELECT username AS old_username
                    FROM usernames
                    WHERE user_id = $1::int4
                )
                INSERT INTO usernames (user_id, username)
                VALUES ($1::int4, $2)
                ON CONFLICT (user_id) DO UPDATE
                    SET username = EXCLUDED.username
                    WHERE usernames.username IS DISTINCT FROM EXCLUDED.username
                RETURNING
                    (xmax = 0)                        AS inserted,
                    (SELECT old_username FROM old)    AS old_username
                "#,
            )
            .bind(uid)
            .bind(username)
            .fetch_optional(&*self.pg)
            .await;

        match result {
            Err(e) => {
                error!("Postgres upsert failed for user {user_id}: {e}");
                // Do NOT update the cache on failure so the next message will
                // retry the DB write rather than silently skipping it.
            }
            // No row returned: username was already identical in the DB.
            // Sync the in-memory cache in case it was stale.
            Ok(None) => {
                self.username_cache
                    .insert(user_id.to_owned(), username.to_owned());
            }
            Ok(Some((inserted, old_username))) => {
                // Update cache after a confirmed successful DB write.
                self.username_cache
                    .insert(user_id.to_owned(), username.to_owned());

                if !inserted {
                    // Row existed and was updated — real name change.
                    // old_username comes from the CTE and is the value that was
                    // in the DB before our upsert, so it is always accurate
                    // regardless of whether the in-memory cache was populated.
                    if let Some(old) = old_username {
                        let ts = Utc::now().timestamp_millis();
                        if let Err(e) = sqlx::query(
                            r#"
                            INSERT INTO username_history (user_id, ts, old_username, new_username)
                            VALUES ($1::int4, $2::int8, $3, $4)
                            "#,
                        )
                        .bind(uid)
                        .bind(ts)
                        .bind(&old)
                        .bind(username)
                        .execute(&*self.pg)
                        .await
                        {
                            error!("Postgres history insert failed for user {user_id}: {e}");
                        } else {
                            info!("Username change: {user_id} {old} -> {username}");
                        }
                    }
                }
                // inserted = true: brand-new user, no history row needed.
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
