//! MCP (Model Context Protocol) server — 骨架实现(M5 完善)

/// 启动 MCP server(阻塞直到 Ctrl+C)
pub async fn run_mcp_server() -> anyhow::Result<()> {
    let port: u16 = std::env::var("XI_MCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9741);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    println!("MCP server listening on 127.0.0.1:{}", port);

    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            let n = match stream.read(&mut buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let _req = String::from_utf8_lossy(&buf[..n]);
            let resp = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"xi_run","description":"Run a dev task"},{"name":"xi_model_list","description":"List models"},{"name":"xi_status","description":"Show status"}]}}"#;
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}
