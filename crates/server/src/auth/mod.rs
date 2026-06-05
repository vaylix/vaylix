use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use engine::StorageKeyring;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Result, ServerError};

mod password;
mod pattern;

use password::{hash_password, password_matches, validate_password_policy};
use pattern::{pattern_covers, wildcard_matches};

pub const DEFAULT_USERNAME: &str = "vaylix";
pub const DEFAULT_PASSWORD: &str = "vaylix";

const AUTH_RESOURCE: &str = "auth metadata";
const AUTH_FORMAT_VERSION: u32 = 2;
const MIN_SUPPORTED_AUTH_FORMAT_VERSION: u32 = 1;
const ADMIN_ROLE: &str = "admin";

/// Coarse command permissions granted to roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,
    Write,
    Admin,
    Backup,
    Restore,
    Metrics,
    Snapshot,
    Clear,
    UserAdmin,
    RoleAdmin,
}

impl Permission {
    pub fn all() -> BTreeSet<Self> {
        [
            Self::Read,
            Self::Write,
            Self::Admin,
            Self::Backup,
            Self::Restore,
            Self::Metrics,
            Self::Snapshot,
            Self::Clear,
            Self::UserAdmin,
            Self::RoleAdmin,
        ]
        .into_iter()
        .collect()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Metrics => "metrics",
            Self::Snapshot => "snapshot",
            Self::Clear => "clear",
            Self::UserAdmin => "user_admin",
            Self::RoleAdmin => "role_admin",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "admin" => Ok(Self::Admin),
            "backup" => Ok(Self::Backup),
            "restore" => Ok(Self::Restore),
            "metrics" => Ok(Self::Metrics),
            "snapshot" => Ok(Self::Snapshot),
            "clear" => Ok(Self::Clear),
            "user_admin" => Ok(Self::UserAdmin),
            "role_admin" => Ok(Self::RoleAdmin),
            _ => Err(ServerError::InvalidPermission(value.to_string())),
        }
    }
}

/// A permission grant scoped to a glob-like key pattern.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PermissionGrant {
    pub permission: Permission,
    pub pattern: String,
}

/// Authenticated session identity with resolved permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub username: String,
    pub permissions: BTreeSet<Permission>,
    pub grants: BTreeSet<PermissionGrant>,
}

impl Identity {
    pub fn has(&self, permission: Permission) -> bool {
        self.permissions.contains(&Permission::Admin) || self.permissions.contains(&permission)
    }

    pub fn allows_key(&self, permission: Permission, key: &str) -> bool {
        self.permissions.contains(&Permission::Admin)
            || self.grants.iter().any(|grant| {
                grant.permission == permission && wildcard_matches(&grant.pattern, key)
            })
    }

    pub fn allows_pattern(&self, permission: Permission, requested_pattern: &str) -> bool {
        self.permissions.contains(&Permission::Admin)
            || self.grants.iter().any(|grant| {
                grant.permission == permission && pattern_covers(&grant.pattern, requested_pattern)
            })
    }

    pub fn permissions_csv(&self) -> String {
        self.permissions
            .iter()
            .map(|permission| permission.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn grants_csv(&self) -> String {
        self.grants
            .iter()
            .map(|grant| format!("{} on {}", grant.permission.as_str(), grant.pattern))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct UserRecord {
    password_hash: String,
    roles: BTreeSet<String>,
    disabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoleRecord {
    #[serde(default)]
    permissions: BTreeSet<Permission>,
    #[serde(default)]
    grants: BTreeSet<PermissionGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BootstrapAdminRecord {
    username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredAuth {
    version: u32,
    users: BTreeMap<String, UserRecord>,
    roles: BTreeMap<String, RoleRecord>,
    #[serde(default)]
    bootstrap_admin: Option<BootstrapAdminRecord>,
}

#[derive(Debug, Clone)]
struct PersistConfig {
    path: PathBuf,
    temp_path: PathBuf,
    keyring: StorageKeyring,
}

/// Shared authentication and authorization store for all server sessions.
#[derive(Clone)]
pub struct AuthConfig {
    store: Arc<RwLock<AuthStore>>,
}

#[derive(Debug, Clone)]
struct AuthStore {
    stored: StoredAuth,
    persist: Option<PersistConfig>,
}

impl AuthConfig {
    /// Builds an in-memory auth config from a bootstrap administrator.
    pub fn new(username: String, password: String) -> Result<Self> {
        Ok(Self {
            store: Arc::new(RwLock::new(AuthStore {
                stored: bootstrap_store(username, password)?,
                persist: None,
            })),
        })
    }

    /// Loads encrypted auth/RBAC metadata or creates a bootstrap admin store.
    pub fn load_or_bootstrap(
        path: PathBuf,
        temp_path: PathBuf,
        keyring: StorageKeyring,
        username: String,
        password: String,
    ) -> Result<Self> {
        let persist = PersistConfig {
            path,
            temp_path,
            keyring,
        };
        let stored = match load_store(&persist)? {
            Some(mut stored) => {
                reconcile_configured_bootstrap_admin(&mut stored, &username, &password)?;
                stored
            }
            None => bootstrap_store(username, password)?,
        };
        let store = AuthStore {
            stored,
            persist: Some(persist),
        };
        store.save()?;
        Ok(Self {
            store: Arc::new(RwLock::new(store)),
        })
    }

    /// Verifies a username/password pair and resolves session permissions.
    pub async fn verify(&self, username: &str, password: &str) -> Result<Option<Identity>> {
        self.store.read().await.verify(username, password)
    }

    pub async fn create_user(&self, username: String, password: String) -> Result<()> {
        self.store.write().await.create_user(username, password)
    }

    pub async fn alter_user_password(&self, username: &str, password: String) -> Result<()> {
        self.store
            .write()
            .await
            .alter_user_password(username, password)
    }

    pub async fn drop_user(&self, username: &str) -> Result<()> {
        self.store.write().await.drop_user(username)
    }

    pub async fn create_role(&self, role: String) -> Result<()> {
        self.store.write().await.create_role(role)
    }

    pub async fn drop_role(&self, role: &str) -> Result<()> {
        self.store.write().await.drop_role(role)
    }

    pub async fn grant_role(&self, role: &str, username: &str) -> Result<()> {
        self.store.write().await.grant_role(role, username)
    }

    pub async fn revoke_role(&self, role: &str, username: &str) -> Result<()> {
        self.store.write().await.revoke_role(role, username)
    }

    pub async fn grant_permission(
        &self,
        permission: Permission,
        pattern: String,
        role: &str,
    ) -> Result<()> {
        self.store
            .write()
            .await
            .grant_permission(permission, pattern, role)
    }

    pub async fn revoke_permission(
        &self,
        permission: Permission,
        pattern: String,
        role: &str,
    ) -> Result<()> {
        self.store
            .write()
            .await
            .revoke_permission(permission, pattern, role)
    }

    pub async fn users(&self) -> Vec<(String, String)> {
        self.store.read().await.users()
    }

    pub async fn roles(&self) -> Vec<(String, String)> {
        self.store.read().await.roles()
    }

    pub async fn grants_for_user(&self, username: &str) -> Result<Vec<(String, String)>> {
        self.store.read().await.grants_for_user(username)
    }

    pub async fn grants_for_role(&self, role: &str) -> Result<Vec<(String, String)>> {
        self.store.read().await.grants_for_role(role)
    }
}

impl AuthStore {
    fn verify(&self, username: &str, password: &str) -> Result<Option<Identity>> {
        let Some(user) = self.stored.users.get(username) else {
            return Ok(None);
        };
        if user.disabled {
            return Ok(None);
        }
        if !password_matches(user, password)? {
            return Ok(None);
        }

        let (permissions, grants) = self.resolve_permissions(user);
        Ok(Some(Identity {
            username: username.to_string(),
            permissions,
            grants,
        }))
    }

    fn create_user(&mut self, username: String, password: String) -> Result<()> {
        validate_password_policy(&password)?;
        if self.stored.users.contains_key(&username) {
            return Err(ServerError::UserAlreadyExists(username));
        }
        self.stored.users.insert(
            username,
            UserRecord {
                password_hash: hash_password(&password)?,
                roles: BTreeSet::new(),
                disabled: false,
            },
        );
        self.save()
    }

    fn alter_user_password(&mut self, username: &str, password: String) -> Result<()> {
        validate_password_policy(&password)?;
        let user = self
            .stored
            .users
            .get_mut(username)
            .ok_or_else(|| ServerError::UserNotFound(username.to_string()))?;
        user.password_hash = hash_password(&password)?;
        self.save()
    }

    fn drop_user(&mut self, username: &str) -> Result<()> {
        if !self.stored.users.contains_key(username) {
            return Err(ServerError::UserNotFound(username.to_string()));
        }
        if self.admin_user_count() <= 1
            && self
                .stored
                .users
                .get(username)
                .is_some_and(|user| user.roles.contains(ADMIN_ROLE))
        {
            return Err(ServerError::LastAdminUser);
        }
        self.stored.users.remove(username);
        self.save()
    }

    fn create_role(&mut self, role: String) -> Result<()> {
        if self.stored.roles.contains_key(&role) {
            return Err(ServerError::RoleAlreadyExists(role));
        }
        self.stored.roles.insert(
            role,
            RoleRecord {
                permissions: BTreeSet::new(),
                grants: BTreeSet::new(),
            },
        );
        self.save()
    }

    fn drop_role(&mut self, role: &str) -> Result<()> {
        if role == ADMIN_ROLE {
            return Err(ServerError::ProtectedRole(role.to_string()));
        }
        if !self.stored.roles.contains_key(role) {
            return Err(ServerError::RoleNotFound(role.to_string()));
        }
        for user in self.stored.users.values_mut() {
            user.roles.remove(role);
        }
        self.stored.roles.remove(role);
        self.save()
    }

    fn grant_role(&mut self, role: &str, username: &str) -> Result<()> {
        if !self.stored.roles.contains_key(role) {
            return Err(ServerError::RoleNotFound(role.to_string()));
        }
        let user = self
            .stored
            .users
            .get_mut(username)
            .ok_or_else(|| ServerError::UserNotFound(username.to_string()))?;
        user.roles.insert(role.to_string());
        self.save()
    }

    fn revoke_role(&mut self, role: &str, username: &str) -> Result<()> {
        let removing_last_admin = role == ADMIN_ROLE
            && self.admin_user_count() <= 1
            && self
                .stored
                .users
                .get(username)
                .is_some_and(|user| user.roles.contains(ADMIN_ROLE));
        if removing_last_admin {
            return Err(ServerError::LastAdminUser);
        }
        let user = self
            .stored
            .users
            .get_mut(username)
            .ok_or_else(|| ServerError::UserNotFound(username.to_string()))?;
        user.roles.remove(role);
        self.save()
    }

    fn grant_permission(
        &mut self,
        permission: Permission,
        pattern: String,
        role: &str,
    ) -> Result<()> {
        let role = self
            .stored
            .roles
            .get_mut(role)
            .ok_or_else(|| ServerError::RoleNotFound(role.to_string()))?;
        role.grants.insert(PermissionGrant {
            permission,
            pattern,
        });
        self.save()
    }

    fn revoke_permission(
        &mut self,
        permission: Permission,
        pattern: String,
        role: &str,
    ) -> Result<()> {
        if role == ADMIN_ROLE && permission == Permission::Admin && pattern == "*" {
            return Err(ServerError::ProtectedRole(role.to_string()));
        }
        let role = self
            .stored
            .roles
            .get_mut(role)
            .ok_or_else(|| ServerError::RoleNotFound(role.to_string()))?;
        role.grants.remove(&PermissionGrant {
            permission,
            pattern: pattern.clone(),
        });
        if pattern == "*" {
            role.permissions.remove(&permission);
        }
        self.save()
    }

    fn users(&self) -> Vec<(String, String)> {
        self.stored
            .users
            .iter()
            .map(|(username, user)| {
                (
                    username.clone(),
                    format!(
                        "roles={} disabled={}",
                        user.roles.iter().cloned().collect::<Vec<_>>().join(","),
                        user.disabled
                    ),
                )
            })
            .collect()
    }

    fn roles(&self) -> Vec<(String, String)> {
        self.stored
            .roles
            .iter()
            .map(|(role, record)| (role.clone(), role_grants(record).join(",")))
            .collect()
    }

    fn grants_for_user(&self, username: &str) -> Result<Vec<(String, String)>> {
        let user = self
            .stored
            .users
            .get(username)
            .ok_or_else(|| ServerError::UserNotFound(username.to_string()))?;
        let mut entries = Vec::new();
        entries.push((
            format!("user.{username}.roles"),
            user.roles.iter().cloned().collect::<Vec<_>>().join(","),
        ));
        let mut index = 0;
        for role in &user.roles {
            if let Some(record) = self.stored.roles.get(role) {
                for grant in role_grants(record) {
                    entries.push((
                        format!("user.{username}.grant.{index:03}"),
                        format!("role={role} {grant}"),
                    ));
                    index += 1;
                }
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    fn grants_for_role(&self, role: &str) -> Result<Vec<(String, String)>> {
        let record = self
            .stored
            .roles
            .get(role)
            .ok_or_else(|| ServerError::RoleNotFound(role.to_string()))?;
        let mut entries = role_grants(record)
            .into_iter()
            .enumerate()
            .map(|(index, grant)| (format!("role.{role}.grant.{index:03}"), grant))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    fn resolve_permissions(
        &self,
        user: &UserRecord,
    ) -> (BTreeSet<Permission>, BTreeSet<PermissionGrant>) {
        let mut permissions = BTreeSet::new();
        let mut grants = BTreeSet::new();
        for role in &user.roles {
            if let Some(role) = self.stored.roles.get(role) {
                permissions.extend(role.permissions.iter().copied());
                grants.extend(
                    role.permissions
                        .iter()
                        .copied()
                        .map(|permission| PermissionGrant {
                            permission,
                            pattern: "*".to_string(),
                        }),
                );
                permissions.extend(
                    role.grants
                        .iter()
                        .filter(|grant| grant.pattern == "*")
                        .map(|grant| grant.permission),
                );
                grants.extend(role.grants.iter().cloned());
            }
        }
        (permissions, grants)
    }

    fn admin_user_count(&self) -> usize {
        self.stored
            .users
            .values()
            .filter(|user| !user.disabled && user.roles.contains(ADMIN_ROLE))
            .count()
    }

    fn save(&self) -> Result<()> {
        let Some(persist) = &self.persist else {
            return Ok(());
        };
        save_store(&self.stored, persist)
    }
}

fn bootstrap_store(username: String, password: String) -> Result<StoredAuth> {
    let mut user_roles = BTreeSet::new();
    user_roles.insert(ADMIN_ROLE.to_string());
    let mut users = BTreeMap::new();
    let bootstrap_username = username.clone();
    users.insert(
        username,
        UserRecord {
            password_hash: hash_password(&password)?,
            roles: user_roles,
            disabled: false,
        },
    );

    Ok(StoredAuth {
        version: AUTH_FORMAT_VERSION,
        users,
        roles: admin_roles(),
        bootstrap_admin: Some(BootstrapAdminRecord {
            username: bootstrap_username,
        }),
    })
}

fn reconcile_configured_bootstrap_admin(
    stored: &mut StoredAuth,
    username: &str,
    password: &str,
) -> Result<()> {
    if username == DEFAULT_USERNAME && password == DEFAULT_PASSWORD {
        stored.version = AUTH_FORMAT_VERSION;
        return Ok(());
    }

    let previous_bootstrap_username = stored
        .bootstrap_admin
        .as_ref()
        .map(|record| record.username.clone());
    let legacy_single_admin_username = legacy_single_admin_username(stored);

    ensure_admin_role(stored);
    let mut roles = BTreeSet::new();
    roles.insert(ADMIN_ROLE.to_string());
    let record = UserRecord {
        password_hash: hash_password(password)?,
        roles,
        disabled: false,
    };

    stored
        .users
        .entry(username.to_string())
        .and_modify(|user| {
            user.password_hash = record.password_hash.clone();
            user.roles.insert(ADMIN_ROLE.to_string());
            user.disabled = false;
        })
        .or_insert(record);

    retire_previous_bootstrap_admin(stored, previous_bootstrap_username.as_deref(), username)?;
    if previous_bootstrap_username.is_none() {
        retire_previous_bootstrap_admin(stored, legacy_single_admin_username.as_deref(), username)?;
    }
    retire_legacy_default_bootstrap_user(stored, username)?;
    stored.bootstrap_admin = Some(BootstrapAdminRecord {
        username: username.to_string(),
    });
    stored.version = AUTH_FORMAT_VERSION;
    Ok(())
}

fn legacy_single_admin_username(stored: &StoredAuth) -> Option<String> {
    if stored.bootstrap_admin.is_some() {
        return None;
    }
    let mut admins = stored
        .users
        .iter()
        .filter(|(_, user)| !user.disabled && user.roles.contains(ADMIN_ROLE))
        .map(|(username, _)| username.clone());
    let username = admins.next()?;
    admins.next().is_none().then_some(username)
}

fn retire_previous_bootstrap_admin(
    stored: &mut StoredAuth,
    previous_username: Option<&str>,
    configured_username: &str,
) -> Result<()> {
    let Some(previous_username) = previous_username else {
        return Ok(());
    };
    if previous_username == configured_username {
        return Ok(());
    }
    remove_admin_user_if_present(stored, previous_username);
    Ok(())
}

fn retire_legacy_default_bootstrap_user(
    stored: &mut StoredAuth,
    configured_username: &str,
) -> Result<()> {
    if configured_username == DEFAULT_USERNAME {
        return Ok(());
    }

    let Some(default_user) = stored.users.get(DEFAULT_USERNAME) else {
        return Ok(());
    };
    if default_user.roles.contains(ADMIN_ROLE) || password_matches(default_user, DEFAULT_PASSWORD)?
    {
        remove_admin_user_if_present(stored, DEFAULT_USERNAME);
    }
    Ok(())
}

fn remove_admin_user_if_present(stored: &mut StoredAuth, username: &str) {
    let Some(user) = stored.users.get(username) else {
        return;
    };
    if user.roles.contains(ADMIN_ROLE) {
        stored.users.remove(username);
    }
}

fn admin_roles() -> BTreeMap<String, RoleRecord> {
    let mut roles = BTreeMap::new();
    roles.insert(ADMIN_ROLE.to_string(), admin_role_record());
    roles
}

fn ensure_admin_role(stored: &mut StoredAuth) {
    let role = stored
        .roles
        .entry(ADMIN_ROLE.to_string())
        .or_insert_with(admin_role_record);
    role.permissions.extend(Permission::all());
    role.grants.extend(
        Permission::all()
            .into_iter()
            .map(|permission| PermissionGrant {
                permission,
                pattern: "*".to_string(),
            }),
    );
}

fn admin_role_record() -> RoleRecord {
    RoleRecord {
        permissions: Permission::all(),
        grants: Permission::all()
            .into_iter()
            .map(|permission| PermissionGrant {
                permission,
                pattern: "*".to_string(),
            })
            .collect(),
    }
}

fn load_store(persist: &PersistConfig) -> Result<Option<StoredAuth>> {
    match fs::read(&persist.path) {
        Ok(bytes) => {
            let decrypted = engine::storage_decrypt(&persist.keyring, AUTH_RESOURCE, &bytes)?;
            let stored: StoredAuth = serde_json::from_slice(&decrypted)
                .map_err(|err| ServerError::AuthStoreDecode(err.to_string()))?;
            if !(MIN_SUPPORTED_AUTH_FORMAT_VERSION..=AUTH_FORMAT_VERSION).contains(&stored.version)
            {
                return Err(ServerError::AuthStoreDecode(format!(
                    "unsupported auth store version {}",
                    stored.version
                )));
            }
            Ok(Some(stored))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn save_store(stored: &StoredAuth, persist: &PersistConfig) -> Result<()> {
    let bytes =
        serde_json::to_vec(stored).map_err(|err| ServerError::AuthStoreEncode(err.to_string()))?;
    let encrypted = engine::storage_encrypt(persist.keyring.active(), AUTH_RESOURCE, &bytes)?;
    let mut file = File::create(&persist.temp_path)?;
    file.write_all(&encrypted)?;
    file.sync_all()?;
    fs::rename(&persist.temp_path, &persist.path)?;
    File::open(&persist.path)?.sync_all()?;
    Ok(())
}

fn role_grants(record: &RoleRecord) -> Vec<String> {
    let mut grants = record
        .permissions
        .iter()
        .map(|permission| format!("{} on *", permission.as_str()))
        .chain(
            record
                .grants
                .iter()
                .map(|grant| format!("{} on {}", grant.permission.as_str(), grant.pattern)),
        )
        .collect::<Vec<_>>();
    grants.sort();
    grants.dedup();
    grants
}

#[allow(dead_code)]
fn _assert_paths(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::{AuthConfig, Permission, PersistConfig, load_store, save_store};
    use engine::{StorageKey, StorageKeyring};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-auth-{name}-{unique}.bin"))
    }

    fn keyring() -> StorageKeyring {
        StorageKeyring {
            active: StorageKey {
                id: Uuid::from_u128(1),
                secret: "auth-test-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        }
    }

    #[tokio::test]
    async fn bootstrap_admin_has_all_permissions() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        let identity = auth.verify("root", "secret").await.unwrap().unwrap();

        assert!(identity.has(Permission::Admin));
        assert!(identity.has(Permission::Read));
    }

    #[tokio::test]
    async fn grants_roles_and_permissions_to_users() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "password1234".to_string())
            .await
            .unwrap();
        auth.create_role("readonly".to_string()).await.unwrap();
        auth.grant_permission(Permission::Read, "*".to_string(), "readonly")
            .await
            .unwrap();
        auth.grant_role("readonly", "alice").await.unwrap();

        let identity = auth.verify("alice", "password1234").await.unwrap().unwrap();
        assert!(identity.has(Permission::Read));
        assert!(identity.allows_key(Permission::Read, "anything"));
        assert!(!identity.has(Permission::Write));
    }

    #[tokio::test]
    async fn grants_pattern_scoped_permissions() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "password1234".to_string())
            .await
            .unwrap();
        auth.create_role("app_reader".to_string()).await.unwrap();
        auth.grant_permission(Permission::Read, "app:*".to_string(), "app_reader")
            .await
            .unwrap();
        auth.grant_role("app_reader", "alice").await.unwrap();

        let identity = auth.verify("alice", "password1234").await.unwrap().unwrap();
        assert!(!identity.has(Permission::Read));
        assert!(identity.allows_key(Permission::Read, "app:1"));
        assert!(identity.allows_pattern(Permission::Read, "app:*"));
        assert!(!identity.allows_key(Permission::Read, "other:1"));
        assert!(!identity.allows_pattern(Permission::Read, "other:*"));
    }

    #[tokio::test]
    async fn wildcard_grants_do_not_cover_unmatched_prefixes() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "password1234".to_string())
            .await
            .unwrap();
        auth.create_role("app_reader".to_string()).await.unwrap();
        auth.grant_permission(Permission::Read, "app:*:prod".to_string(), "app_reader")
            .await
            .unwrap();
        auth.grant_role("app_reader", "alice").await.unwrap();

        let identity = auth.verify("alice", "password1234").await.unwrap().unwrap();
        assert!(identity.allows_key(Permission::Read, "app:one:prod"));
        assert!(!identity.allows_key(Permission::Read, "app:one:dev"));
        assert!(!identity.allows_key(Permission::Read, "platform:one:prod"));
        assert!(!identity.allows_pattern(Permission::Read, "*"));
    }

    #[tokio::test]
    async fn unicode_distinct_usernames_do_not_alias() {
        let composed = "é".to_string();
        let decomposed = "e\u{301}".to_string();
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user(composed.clone(), "password1234".to_string())
            .await
            .unwrap();
        auth.create_user(decomposed.clone(), "password5678".to_string())
            .await
            .unwrap();

        assert!(
            auth.verify(&composed, "password1234")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            auth.verify(&composed, "password5678")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            auth.verify(&decomposed, "password5678")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            auth.verify(&decomposed, "password1234")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_password_and_role_mutation_preserves_admin_access() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "password0000".to_string())
            .await
            .unwrap();

        let password_auth = auth.clone();
        let password_task = tokio::spawn(async move {
            for index in 1..=20 {
                password_auth
                    .alter_user_password("alice", format!("password{index:04}"))
                    .await
                    .unwrap();
            }
        });

        let role_auth = auth.clone();
        let role_task = tokio::spawn(async move {
            for index in 0..20 {
                let role = format!("role-{index:02}");
                role_auth.create_role(role.clone()).await.unwrap();
                role_auth
                    .grant_permission(Permission::Read, format!("tenant-{index}:*"), &role)
                    .await
                    .unwrap();
                role_auth.grant_role(&role, "alice").await.unwrap();
            }
        });

        password_task.await.unwrap();
        role_task.await.unwrap();

        assert!(auth.verify("root", "secret").await.unwrap().is_some());
        assert!(
            auth.verify("alice", "password0020")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rotates_password_for_future_auth_attempts() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "oldpassword1234".to_string())
            .await
            .unwrap();

        let existing = auth
            .verify("alice", "oldpassword1234")
            .await
            .unwrap()
            .unwrap();
        auth.alter_user_password("alice", "newpassword1234".to_string())
            .await
            .unwrap();

        assert_eq!(existing.username, "alice");
        assert!(
            auth.verify("alice", "oldpassword1234")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            auth.verify("alice", "newpassword1234")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn persists_encrypted_auth_store() {
        let path = temp_path("store");
        let temp = temp_path("store-tmp");
        let auth = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "root".to_string(),
            "secret".to_string(),
        )
        .unwrap();
        auth.create_user("alice".to_string(), "password1234".to_string())
            .await
            .unwrap();
        auth.alter_user_password("alice", "rotated12345".to_string())
            .await
            .unwrap();

        let raw = fs::read(&path).unwrap();
        assert!(!String::from_utf8_lossy(&raw).contains("alice"));

        let reloaded = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "ignored".to_string(),
            "ignored".to_string(),
        )
        .unwrap();
        assert!(
            reloaded
                .verify("alice", "password1234")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reloaded
                .verify("alice", "rotated12345")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[tokio::test]
    async fn reconciles_non_default_bootstrap_password_into_persisted_store() {
        let path = temp_path("bootstrap-password");
        let temp = temp_path("bootstrap-password-tmp");
        let auth = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "vaylix".to_string(),
            "vaylix".to_string(),
        )
        .unwrap();
        assert!(auth.verify("vaylix", "vaylix").await.unwrap().is_some());

        let reloaded = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "vaylix".to_string(),
            "7965PPO4".to_string(),
        )
        .unwrap();
        assert!(reloaded.verify("vaylix", "vaylix").await.unwrap().is_none());
        assert!(
            reloaded
                .verify("vaylix", "7965PPO4")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[tokio::test]
    async fn retires_default_bootstrap_user_when_configured_admin_changes() {
        let path = temp_path("bootstrap-user");
        let temp = temp_path("bootstrap-user-tmp");
        AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "vaylix".to_string(),
            "vaylix".to_string(),
        )
        .unwrap();

        let reloaded = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "admin".to_string(),
            "7965PPO4".to_string(),
        )
        .unwrap();
        assert!(reloaded.verify("vaylix", "vaylix").await.unwrap().is_none());
        assert!(
            reloaded
                .verify("admin", "7965PPO4")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[tokio::test]
    async fn retires_previous_env_managed_admin_when_configured_user_changes() {
        let path = temp_path("bootstrap-user-rotation");
        let temp = temp_path("bootstrap-user-rotation-tmp");
        let first = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "vaylix".to_string(),
            "firstpass1234".to_string(),
        )
        .unwrap();
        assert!(
            first
                .verify("vaylix", "firstpass1234")
                .await
                .unwrap()
                .is_some()
        );

        let second = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "admin".to_string(),
            "secondpass1234".to_string(),
        )
        .unwrap();

        assert!(
            second
                .verify("vaylix", "firstpass1234")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            second
                .verify("admin", "secondpass1234")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[tokio::test]
    async fn rotates_tracked_env_admin_password_on_restart() {
        let path = temp_path("bootstrap-password-rotation");
        let temp = temp_path("bootstrap-password-rotation-tmp");
        AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "admin".to_string(),
            "firstpass1234".to_string(),
        )
        .unwrap();

        let reloaded = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "admin".to_string(),
            "secondpass1234".to_string(),
        )
        .unwrap();

        assert!(
            reloaded
                .verify("admin", "firstpass1234")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reloaded
                .verify("admin", "secondpass1234")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[tokio::test]
    async fn retires_legacy_single_admin_when_configured_user_changes() {
        let path = temp_path("legacy-bootstrap-user-rotation");
        let temp = temp_path("legacy-bootstrap-user-rotation-tmp");
        AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "firstadmin".to_string(),
            "firstpass1234".to_string(),
        )
        .unwrap();

        let persist = PersistConfig {
            path: path.clone(),
            temp_path: temp.clone(),
            keyring: keyring(),
        };
        let mut stored = load_store(&persist).unwrap().unwrap();
        stored.version = 1;
        stored.bootstrap_admin = None;
        save_store(&stored, &persist).unwrap();

        let reloaded = AuthConfig::load_or_bootstrap(
            path.clone(),
            temp.clone(),
            keyring(),
            "secondadmin".to_string(),
            "secondpass1234".to_string(),
        )
        .unwrap();

        assert!(
            reloaded
                .verify("firstadmin", "firstpass1234")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reloaded
                .verify("secondadmin", "secondpass1234")
                .await
                .unwrap()
                .is_some()
        );

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }
}
