use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use engine::StorageKeyring;
use rand::random;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Result, ServerError};

pub const DEFAULT_USERNAME: &str = "vaylix";
pub const DEFAULT_PASSWORD: &str = "vaylix";

const AUTH_RESOURCE: &str = "auth metadata";
const AUTH_FORMAT_VERSION: u32 = 1;
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
struct StoredAuth {
    version: u32,
    users: BTreeMap<String, UserRecord>,
    roles: BTreeMap<String, RoleRecord>,
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
        let stored = load_store(&persist)?.unwrap_or(bootstrap_store(username, password)?);
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
}

impl AuthStore {
    fn verify(&self, username: &str, password: &str) -> Result<Option<Identity>> {
        let Some(user) = self.stored.users.get(username) else {
            return Ok(None);
        };
        if user.disabled {
            return Ok(None);
        }
        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|_| ServerError::AuthenticationConfiguration)?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_err()
        {
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
    let mut roles = BTreeMap::new();
    roles.insert(
        ADMIN_ROLE.to_string(),
        RoleRecord {
            permissions: Permission::all(),
            grants: Permission::all()
                .into_iter()
                .map(|permission| PermissionGrant {
                    permission,
                    pattern: "*".to_string(),
                })
                .collect(),
        },
    );

    let mut user_roles = BTreeSet::new();
    user_roles.insert(ADMIN_ROLE.to_string());
    let mut users = BTreeMap::new();
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
        roles,
    })
}

fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::encode_b64(&random::<[u8; 16]>())
        .map_err(|_| ServerError::AuthenticationConfiguration)?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| ServerError::AuthenticationConfiguration)?
        .to_string())
}

fn load_store(persist: &PersistConfig) -> Result<Option<StoredAuth>> {
    match fs::read(&persist.path) {
        Ok(bytes) => {
            let decrypted = engine::storage_decrypt(&persist.keyring, AUTH_RESOURCE, &bytes)?;
            let stored: StoredAuth = serde_json::from_slice(&decrypted)
                .map_err(|err| ServerError::AuthStoreDecode(err.to_string()))?;
            if stored.version != AUTH_FORMAT_VERSION {
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

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let Some(star_index) = pattern.find('*') else {
        return pattern == value;
    };
    let (prefix, suffix_with_star) = pattern.split_at(star_index);
    let suffix = &suffix_with_star[1..];
    value.starts_with(prefix) && value.ends_with(suffix)
}

fn pattern_covers(grant_pattern: &str, requested_pattern: &str) -> bool {
    grant_pattern == "*"
        || grant_pattern == requested_pattern
        || requested_pattern != "*"
            && grant_pattern.ends_with('*')
            && requested_pattern.starts_with(grant_pattern.trim_end_matches('*'))
}

#[allow(dead_code)]
fn _assert_paths(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::{AuthConfig, Permission};
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
        auth.create_user("alice".to_string(), "pw".to_string())
            .await
            .unwrap();
        auth.create_role("readonly".to_string()).await.unwrap();
        auth.grant_permission(Permission::Read, "*".to_string(), "readonly")
            .await
            .unwrap();
        auth.grant_role("readonly", "alice").await.unwrap();

        let identity = auth.verify("alice", "pw").await.unwrap().unwrap();
        assert!(identity.has(Permission::Read));
        assert!(identity.allows_key(Permission::Read, "anything"));
        assert!(!identity.has(Permission::Write));
    }

    #[tokio::test]
    async fn grants_pattern_scoped_permissions() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "pw".to_string())
            .await
            .unwrap();
        auth.create_role("app_reader".to_string()).await.unwrap();
        auth.grant_permission(Permission::Read, "app:*".to_string(), "app_reader")
            .await
            .unwrap();
        auth.grant_role("app_reader", "alice").await.unwrap();

        let identity = auth.verify("alice", "pw").await.unwrap().unwrap();
        assert!(!identity.has(Permission::Read));
        assert!(identity.allows_key(Permission::Read, "app:1"));
        assert!(identity.allows_pattern(Permission::Read, "app:*"));
        assert!(!identity.allows_key(Permission::Read, "other:1"));
        assert!(!identity.allows_pattern(Permission::Read, "other:*"));
    }

    #[tokio::test]
    async fn rotates_password_for_future_auth_attempts() {
        let auth = AuthConfig::new("root".to_string(), "secret".to_string()).unwrap();
        auth.create_user("alice".to_string(), "old".to_string())
            .await
            .unwrap();

        let existing = auth.verify("alice", "old").await.unwrap().unwrap();
        auth.alter_user_password("alice", "new".to_string())
            .await
            .unwrap();

        assert_eq!(existing.username, "alice");
        assert!(auth.verify("alice", "old").await.unwrap().is_none());
        assert!(auth.verify("alice", "new").await.unwrap().is_some());
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
        auth.create_user("alice".to_string(), "pw".to_string())
            .await
            .unwrap();
        auth.alter_user_password("alice", "rotated".to_string())
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
        assert!(reloaded.verify("alice", "pw").await.unwrap().is_none());
        assert!(reloaded.verify("alice", "rotated").await.unwrap().is_some());

        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }
}
