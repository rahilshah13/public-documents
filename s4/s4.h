#ifndef S4_H
#define S4_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sqlite3.h>

#define S4_VERSION "1.3.0"
#define S4_DEFAULT_PORT 8080
#define S4_MAX_KEY_LEN 1024
#define S4_MAX_BUCKET_LEN 256
#define S4_MAX_TENANT_LEN 64
#define S4_MAX_NODE_ID_LEN 128
#define S4_ACME_TOKEN_LEN 256
#define S4_MAX_EMAIL_LEN 256
#define S4_ACCESS_KEY_LEN 64
#define S4_SECRET_KEY_LEN 128
#define S4_MAX_QUERY_LEN 2048
#define S4_REPLICATION_FACTOR 2
#define S4_MAX_URL_LEN 2048

typedef enum {
    S4_BACKEND_DISK = 0,
    S4_BACKEND_MEMORY = 1,
    S4_BACKEND_REMOTE_NODE = 2,
    S4_BACKEND_GIT = 3
} s4_backend_type_t;

typedef enum {
    S4_PERM_READ = 1 << 0,
    S4_PERM_WRITE = 1 << 1,
    S4_PERM_LIST = 1 << 2,
    S4_PERM_GIT = 1 << 3
} s4_permission_t;

typedef enum {
    S4_EVENT_OBJECT_CREATED = 1 << 0,
    S4_EVENT_OBJECT_DELETED = 1 << 1,
    S4_EVENT_BUCKET_CREATED = 1 << 2,
    S4_EVENT_GIT_PUSH = 1 << 3
} s4_event_type_t;

typedef struct {
    uint64_t event_id;
    s4_event_type_t type;
    char tenant_id[S4_MAX_TENANT_LEN];
    char bucket[S4_MAX_BUCKET_LEN];
    char key[S4_MAX_KEY_LEN];
    uint64_t timestamp;
} s4_event_t;

typedef struct {
    char node_id[S4_MAX_NODE_ID_LEN];
    char storage_path[512];
    char db_path[512];
    char certs_path[512];
    char domain[256];
    char contact_email[S4_MAX_EMAIL_LEN];
    s4_backend_type_t active_backend;
    int port;
    bool debug_mode;
    sqlite3 *db_handle;
} s4_config_t;

typedef struct {
    char tenant_id[S4_MAX_TENANT_LEN];
    char owner_tenant_id[S4_MAX_TENANT_LEN];
    char bucket[S4_MAX_BUCKET_LEN];
    char key[S4_MAX_KEY_LEN];
    size_t size;
    uint64_t last_modified;
    char etag[33];
    bool has_range;
    off_t range_start;
    off_t range_end;
    char assigned_node_id[S4_MAX_NODE_ID_LEN];
} s4_object_t;

typedef struct {
    char account_id[S4_MAX_TENANT_LEN];
    char email[S4_MAX_EMAIL_LEN];
    bool is_confirmed;
} s4_account_t;

typedef struct {
    char access_key_id[S4_ACCESS_KEY_LEN];
    char secret_access_key[S4_SECRET_KEY_LEN];
    char account_id[S4_MAX_TENANT_LEN];
    bool is_active;
} s4_credential_t;

typedef struct {
    uint64_t wal_offset;
    uint32_t shard_ids[S4_REPLICATION_FACTOR];
    uint64_t byte_positions[S4_REPLICATION_FACTOR];
    size_t length;
    uint64_t timestamp;
} s4_index_entry_t;

typedef size_t (*s4_read_callback_t)(void *buffer, size_t max_size, void *user_data);
typedef size_t (*s4_write_callback_t)(const void *chunk, size_t size, void *user_data);

int s4_acme_init_db(sqlite3 *db);
bool s4_acme_store_challenge(sqlite3 *db, const char *token, const char *key_auth);
bool s4_acme_get_challenge(sqlite3 *db, const char *token, char *out_key_auth, size_t max_len);
int s4_acme_start_background_loop(s4_config_t *config);

int s4_account_create(sqlite3 *db, const char *account_id, const char *email, const char *password_hash);
int s4_account_confirm(sqlite3 *db, const char *confirmation_token);
int s4_email_send_confirmation(const char *email, const char *confirmation_token);
int s4_bucket_share(sqlite3 *db, const char *owner_tenant_id, const char *bucket, const char *target_account_id, uint32_t permissions);
bool s4_bucket_check_access(sqlite3 *db, const char *tenant_id, const char *bucket, s4_permission_t required_perm);

int s4_event_init_db(sqlite3 *db);
int s4_event_publish(sqlite3 *db, s4_event_type_t type, const char *tenant_id, const char *bucket, const char *key);
int s4_event_poll(sqlite3 *db, const char *tenant_id, const char *bucket, uint64_t after_event_id, s4_write_callback_t write_cb, void *user_data);

int s4_credential_create(sqlite3 *db, const char *account_id, char *out_access_key, char *out_secret_key);
bool s4_credential_verify(sqlite3 *db, const char *access_key_id, const char *signature, const char *payload_hash, const char *string_to_sign, s4_credential_t *out_cred);

/* Presigned URL Support */
int s4_presigned_url_generate(const char *base_url, const char *method, const char *bucket, const char *key, 
                              const char *access_key_id, const char *secret_access_key, 
                              uint32_t expiration_seconds, char *out_url, size_t max_url_len);
bool s4_presigned_verify(sqlite3 *db, const char *method, const char *uri, const char *query_string, s4_credential_t *out_cred);

int s4_db_init(s4_config_t *config);
void s4_db_close(s4_config_t *config);
bool s4_tenant_verify_exists(sqlite3 *db, const char *tenant_id);
bool s4_bucket_create(sqlite3 *db, const char *tenant_id, const char *bucket);
int s4_tenant_list_buckets(sqlite3 *db, const char *tenant_id, s4_write_callback_t write_cb, void *user_data);
int s4_bucket_list_keys(sqlite3 *db, const char *tenant_id, const char *bucket, s4_write_callback_t write_cb, void *user_data);

int s4_prolog_export_tenant_kb(sqlite3 *db, const char *tenant_id);
int s4_prolog_export_bucket_kb(sqlite3 *db, const char *tenant_id, const char *bucket);
int s4_prolog_get_knowledge_bases(sqlite3 *db, const char *tenant_id, s4_write_callback_t write_cb, void *user_data);
int s4_prolog_get_predicates_human_readable(sqlite3 *db, const char *tenant_id, s4_write_callback_t write_cb, void *user_data);
int s4_prolog_run_query(sqlite3 *db, const char *tenant_id, const char *query_name, s4_write_callback_t write_cb, void *user_data);

int s4_wal_write_replicated(const s4_object_t *obj, s4_read_callback_t read_cb, void *user_data, s4_index_entry_t *out_idx);
int s4_wal_read_replicated(const s4_object_t *obj, const s4_index_entry_t *idx, s4_write_callback_t write_cb, void *user_data);

bool s4_auth_verify(sqlite3 *db, const char *auth_header, const char *method, const char *uri, const char *tenant_id);
bool s4_auth_check_acl(sqlite3 *db, const char *tenant_id, const char *bucket, const char *action);

int s4_storage_init(s4_config_t *config);
void s4_storage_shutdown(void);
int s4_storage_put_stream(const s4_object_t *obj, s4_read_callback_t read_cb, void *user_data);
int s4_storage_get_stream(const s4_object_t *obj, s4_write_callback_t write_cb, void *user_data);
int s4_storage_delete(const s4_object_t *obj);

int s4_git_init(s4_config_t *config);
void s4_git_shutdown(void);
bool s4_git_repo_init(const char *tenant_id, const char *bucket);
int s4_git_handle_info_refs(const char *tenant_id, const char *bucket, const char *service, s4_write_callback_t write_cb, void *user_data);
int s4_git_handle_service(const char *tenant_id, const char *bucket, const char *service, s4_read_callback_t read_cb, s4_write_callback_t write_cb, void *user_data);

int s4_gateway_init(s4_config_t *config);
int s4_gateway_run(void);
void s4_gateway_shutdown(void);

#endif // S4_H