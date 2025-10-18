use actix_cors::Cors;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Result};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize)]
struct NumberResponse {
    number: u64,
    ip: String,
    timestamp: u64,
    is_new: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct NextNumberRequest {
    ip: String,
}

struct AppState {
    db: Arc<Db>,
    lock: Arc<Mutex<()>>,
}

async fn get_next_number(
    query: web::Query<NextNumberRequest>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    // Acquire lock to ensure atomic operation
    let _lock = data.lock.lock().await;

    // Get IP from query parameter
    let ip = &query.ip;

    // Validate IP format (basic validation)
    if ip.is_empty() {
        return Ok(HttpResponse::BadRequest().json(ErrorResponse {
            error: "IP address is required".to_string(),
        }));
    }

    // Key for storing the assigned number for this IP
    let ip_key = format!("ip:{}", ip);

    // Check if this IP already has a number assigned
    if let Some(existing_bytes) = data.db.get(ip_key.as_bytes()).unwrap() {
        // IP already has a number, return the same number
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&existing_bytes[..8]);
        let assigned_number = u64::from_be_bytes(buf);

        // Get the original timestamp
        let mut ts_buf = [0u8; 8];
        ts_buf.copy_from_slice(&existing_bytes[8..16]);
        let timestamp = u64::from_be_bytes(ts_buf);

        return Ok(HttpResponse::Ok().json(NumberResponse {
            number: assigned_number,
            ip: ip.to_string(),
            timestamp,
            is_new: false,
        }));
    }

    // This is a new IP, assign a new number
    // Get the global counter
    let counter_key = b"global_counter";

    let current_value = match data.db.get(counter_key).unwrap() {
        Some(bytes) => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }
        None => {
            // First time, initialize with 99 (will be incremented to 100)
            99
        }
    };

    // Calculate next number
    let next_number = current_value + 1;

    // Update global counter
    data.db
        .insert(counter_key, &next_number.to_be_bytes())
        .unwrap();

    // Get current timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Store the assigned number for this IP (number + timestamp)
    let mut ip_data = Vec::new();
    ip_data.extend_from_slice(&next_number.to_be_bytes());
    ip_data.extend_from_slice(&timestamp.to_be_bytes());

    data.db.insert(ip_key.as_bytes(), ip_data).unwrap();

    // Also store reverse mapping (number -> IP) for lookups
    let number_key = format!("number:{}", next_number);
    data.db
        .insert(number_key.as_bytes(), ip.as_bytes())
        .unwrap();

    // Ensure data is persisted
    data.db.flush().unwrap();

    Ok(HttpResponse::Ok().json(NumberResponse {
        number: next_number,
        ip: ip.to_string(),
        timestamp,
        is_new: true,
    }))
}

async fn get_status(data: web::Data<AppState>) -> Result<HttpResponse> {
    let counter_key = b"global_counter";

    let last_assigned = match data.db.get(counter_key).unwrap() {
        Some(bytes) => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes);
            u64::from_be_bytes(buf)
        }
        None => 99,
    };

    // Count total IPs assigned
    let mut total_ips = 0;
    for item in data.db.iter() {
        let (key, _) = item.unwrap();
        let key_str = String::from_utf8_lossy(&key);
        if key_str.starts_with("ip:") {
            total_ips += 1;
        }
    }

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "last_assigned_number": if last_assigned == 99 { 0 } else { last_assigned },
        "next_available_number": last_assigned + 1,
        "total_ips_registered": total_ips,
        "numbers_available": u64::MAX - last_assigned - 1
    })))
}

async fn get_all_assignments(data: web::Data<AppState>) -> Result<HttpResponse> {
    let mut assignments = Vec::new();

    for item in data.db.iter() {
        let (key, value) = item.unwrap();
        let key_str = String::from_utf8_lossy(&key);

        if key_str.starts_with("ip:") {
            let ip = key_str.strip_prefix("ip:").unwrap().to_string();

            let mut buf = [0u8; 8];
            buf.copy_from_slice(&value[..8]);
            let number = u64::from_be_bytes(buf);

            let mut ts_buf = [0u8; 8];
            ts_buf.copy_from_slice(&value[8..16]);
            let timestamp = u64::from_be_bytes(ts_buf);

            assignments.push(serde_json::json!({
                "ip": ip,
                "number": number,
                "timestamp": timestamp,
                "assigned_at": chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "Unknown".to_string())
            }));
        }
    }

    // Sort by number
    assignments.sort_by_key(|a| a["number"].as_u64().unwrap());

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "total_assignments": assignments.len(),
        "assignments": assignments
    })))
}

async fn lookup_ip(path: web::Path<String>, data: web::Data<AppState>) -> Result<HttpResponse> {
    let ip = path.into_inner();

    let ip_key = format!("ip:{}", ip);

    match data.db.get(ip_key.as_bytes()).unwrap() {
        Some(value) => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&value[..8]);
            let number = u64::from_be_bytes(buf);

            let mut ts_buf = [0u8; 8];
            ts_buf.copy_from_slice(&value[8..16]);
            let timestamp = u64::from_be_bytes(ts_buf);

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "ip": ip,
                "number": number,
                "timestamp": timestamp,
                "assigned_at": chrono::DateTime::from_timestamp(timestamp as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "Unknown".to_string()),
                "status": "assigned"
            })))
        }
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "ip": ip,
            "number": null,
            "status": "not_assigned",
            "message": "This IP has not been assigned a number yet"
        }))),
    }
}

async fn lookup_number(path: web::Path<u64>, data: web::Data<AppState>) -> Result<HttpResponse> {
    let number = path.into_inner();

    let number_key = format!("number:{}", number);

    match data.db.get(number_key.as_bytes()).unwrap() {
        Some(ip_bytes) => {
            let ip = String::from_utf8_lossy(&ip_bytes).to_string();

            // Get additional info about this assignment
            let ip_key = format!("ip:{}", ip);
            if let Some(value) = data.db.get(ip_key.as_bytes()).unwrap() {
                let mut ts_buf = [0u8; 8];
                ts_buf.copy_from_slice(&value[8..16]);
                let timestamp = u64::from_be_bytes(ts_buf);

                Ok(HttpResponse::Ok().json(serde_json::json!({
                    "number": number,
                    "ip": ip,
                    "timestamp": timestamp,
                    "assigned_at": chrono::DateTime::from_timestamp(timestamp as i64, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| "Unknown".to_string()),
                    "status": "assigned"
                })))
            } else {
                Ok(HttpResponse::Ok().json(serde_json::json!({
                    "number": number,
                    "ip": ip,
                    "status": "assigned"
                })))
            }
        }
        None => Ok(HttpResponse::Ok().json(serde_json::json!({
            "number": number,
            "status": "not_assigned",
            "message": "This number has not been assigned to any IP yet"
        }))),
    }
}

async fn reset_all(data: web::Data<AppState>) -> Result<HttpResponse> {
    let _lock = data.lock.lock().await;

    // Clear all data
    data.db.clear().unwrap();
    data.db.flush().unwrap();

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": "All data reset successfully",
        "next_number": 100
    })))
}

async fn delete_ip_assignment(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let _lock = data.lock.lock().await;

    let ip = path.into_inner();
    let ip_key = format!("ip:{}", ip);

    // First, get the number assigned to this IP
    match data.db.get(ip_key.as_bytes()).unwrap() {
        Some(value) => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&value[..8]);
            let number = u64::from_be_bytes(buf);

            // Remove IP assignment
            data.db.remove(ip_key.as_bytes()).unwrap();

            // Remove number mapping
            let number_key = format!("number:{}", number);
            data.db.remove(number_key.as_bytes()).unwrap();

            data.db.flush().unwrap();

            Ok(HttpResponse::Ok().json(serde_json::json!({
                "message": format!("Assignment deleted for IP: {}", ip),
                "ip": ip,
                "deleted_number": number
            })))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "IP not found",
            "ip": ip
        }))),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting Sequential Number API Server...");
    println!("Each IP gets ONE unique number permanently assigned.");

    // Open or create the embedded database
    let db = sled::open("ip_number_assignments.db").expect("Failed to open database");

    // Create shared application state
    let app_state = web::Data::new(AppState {
        db: Arc::new(db),
        lock: Arc::new(Mutex::new(())),
    });

    println!("\nServer running at http://127.0.0.1:8080");
    println!("\nAvailable endpoints:");
    println!("  GET  /next?ip=<IP>         - Get assigned number (or assign new if first time)");
    println!("  GET  /status               - Get system status");
    println!("  GET  /assignments          - List all IP-number assignments");
    println!("  GET  /lookup/ip/<IP>       - Lookup number for specific IP");
    println!("  GET  /lookup/number/<NUM>  - Lookup IP for specific number");
    println!("  DELETE /ip/<IP>            - Delete assignment for IP (admin)");
    println!("  POST /reset                - Reset all data (admin)");
    println!("\nExample: curl 'http://127.0.0.1:8080/next?ip=192.168.1.100'");
    println!("         (First call assigns 100, subsequent calls return 100)");

    HttpServer::new(move || {
        // Define CORS policy
        let cors = Cors::default()
            .allow_any_origin() // or restrict: .allowed_origin("http://localhost:3000")
            .allow_any_method()
            .allow_any_header()
            .supports_credentials();

        App::new()
            .wrap(cors) // 👈 apply CORS middleware
            .app_data(app_state.clone())
            .route("/next", web::get().to(get_next_number))
            .route("/status", web::get().to(get_status))
            .route("/assignments", web::get().to(get_all_assignments))
            .route("/lookup/ip/{ip}", web::get().to(lookup_ip))
            .route("/lookup/number/{number}", web::get().to(lookup_number))
            .route("/ip/{ip}", web::delete().to(delete_ip_assignment))
            .route("/reset", web::post().to(reset_all))
            .route(
                "/",
                web::get().to(|| async {
                    HttpResponse::Ok().json(serde_json::json!({
                        "service": "IP-Number Assignment API",
                        "description": "Each IP gets ONE unique number permanently assigned",
                        "endpoints": {
                            "GET /next?ip=<IP>": "Get your assigned number (assigns one if new IP)",
                            "GET /status": "System status and statistics",
                            "GET /assignments": "List all IP-number assignments",
                            "GET /lookup/ip/<IP>": "Lookup number for specific IP",
                            "GET /lookup/number/<NUM>": "Lookup IP for specific number",
                            "DELETE /ip/<IP>": "Delete assignment for IP",
                            "POST /reset": "Reset all data"
                        },
                        "example": "curl 'http://127.0.0.1:8080/next?ip=192.168.1.100'"
                    }))
                }),
            )
    })
    .bind("0.0.0.0:10000")?
    .run()
    .await
}
