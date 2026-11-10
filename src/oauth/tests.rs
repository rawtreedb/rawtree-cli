use super::*;

#[test]
fn callback_requires_exact_state_and_unambiguous_code() {
    let redirect = "http://127.0.0.1:3456/oauth/callback";
    assert_eq!(
        callback_result("/oauth/callback?code=abc&state=secret", redirect, "secret").unwrap(),
        Some("abc".into())
    );
    for target in [
        "/other?code=abc&state=secret",
        "/oauth/callback?code=abc",
        "/oauth/callback?code=abc&state=wrong",
        "/oauth/callback?code=abc&state=secret&state=wrong",
        "/oauth/callback?code=abc&code=def&state=secret",
        "/oauth/callback?code=abc&error=access_denied&state=secret",
        "/oauth/callback?code=&state=secret",
        "/oauth/callback?error=access_denied&state=wrong",
    ] {
        assert!(
            callback_result(target, redirect, "secret")
                .unwrap()
                .is_none(),
            "{target}"
        );
    }
    assert!(callback_result(
        "/oauth/callback?error=access_denied&state=secret",
        redirect,
        "secret"
    )
    .is_err());
}

#[test]
fn oauth_urls_require_secure_transport_and_same_origin() {
    let issuer = secure_url("https://api.rawtree.com").unwrap();
    assert!(endpoint("https://api.rawtree.com/oauth/token", &issuer).is_ok());
    assert!(endpoint("https://attacker.test/oauth/token", &issuer).is_err());
    for value in [
        "http://api.rawtree.com",
        "https://user:secret@api.rawtree.com",
        "https://api.rawtree.com/#fragment",
        "https://api.rawtree.com/?token=secret",
    ] {
        assert!(secure_url(value).is_err());
    }
    assert!(secure_url("http://127.0.0.1:1234").is_ok());
}

#[test]
fn pkce_matches_rfc7636_vector() {
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    assert_eq!(
        URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes())),
        "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    );
    let secret = random_secret().unwrap();
    assert_eq!(secret.len(), 43);
    assert_ne!(secret, random_secret().unwrap());
}

#[test]
fn transport_rejects_each_unsafe_url_component_independently() {
    for value in [
        "https://api.rawtree.com",
        "http://localhost:1234",
        "http://127.0.0.1:1234",
        "http://[::1]:1234",
    ] {
        assert!(secure_url(value).is_ok(), "{value}");
    }
    for value in [
        "http://example.test",
        "http://192.0.2.1",
        "http://[2001:db8::1]",
        "file:///path",
        "ftp://localhost",
        "https://user@api.rawtree.com",
        "https://:password@api.rawtree.com",
        "https://api.rawtree.com#x",
        "https://api.rawtree.com?x",
    ] {
        assert!(secure_url(value).is_err(), "{value}");
    }
    let issuer = secure_url("http://localhost:1234").unwrap();
    for value in [
        "https://localhost:1234/token",
        "http://localhost:4321/token",
        "http://127.0.0.1:1234/token",
    ] {
        assert!(endpoint(value, &issuer).is_err(), "{value}");
    }
}

#[test]
fn callback_accepts_unrelated_fields_and_distinguishes_denial_from_other_errors() {
    let redirect = "http://127.0.0.1/oauth/callback";
    assert_eq!(
        callback_result(
            "/oauth/callback?code=abc&state=s&extra=allowed",
            redirect,
            "s"
        )
        .unwrap(),
        Some("abc".into())
    );
    for target in [
        "http://127.0.0.1/oauth/callback?code=abc&state=s",
        "/oauth/callback?state=s",
        "/oauth/callback?state=s&error=one&error=two",
    ] {
        assert!(callback_result(target, redirect, "s").unwrap().is_none());
    }
    assert!(
        callback_result("/oauth/callback?state=s&error=access_denied", redirect, "s")
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert!(
        callback_result("/oauth/callback?state=s&error=server_error", redirect, "s")
            .unwrap_err()
            .to_string()
            .contains("failed")
    );
}

fn socket_with_request(request: &[u8]) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    sender.write_all(request).unwrap();
    sender.shutdown(std::net::Shutdown::Write).unwrap();
    listener.accept().unwrap().0
}

#[test]
fn callback_request_line_accepts_both_http_versions_and_rejects_bad_framing() {
    for version in ["HTTP/1.0", "HTTP/1.1"] {
        let mut stream = socket_with_request(
            format!("GET /oauth/callback?code=abc {version}\r\n\r\n").as_bytes(),
        );
        assert_eq!(
            read_callback(&mut stream, Instant::now() + Duration::from_secs(1)).unwrap(),
            "/oauth/callback?code=abc"
        );
    }
    for line in [
        "POST /oauth/callback HTTP/1.1\r\n",
        "GET /oauth/callback HTTP/2\r\n",
        "GET /oauth/callback HTTP/1.1\n",
        "GET /oauth/callback HTTP/1.1\nX-Test: value\r\n\r\n",
        "GET /oauth/callback HTTP/1.1",
        "GET /oauth/callback\r\n",
        "",
        "GET /oauth/callback HTTP/1.1 extra\r\n",
    ] {
        let mut stream = socket_with_request(line.as_bytes());
        assert!(
            read_callback(&mut stream, Instant::now() + Duration::from_secs(1)).is_err(),
            "{line}"
        );
    }
    let mut oversized = socket_with_request(&vec![b'x'; 8192]);
    assert!(read_callback(&mut oversized, Instant::now() + Duration::from_secs(1)).is_err());
    let mut invalid_utf8 = socket_with_request(&[0xff, b'\r', b'\n']);
    assert!(read_callback(&mut invalid_utf8, Instant::now() + Duration::from_secs(1)).is_err());
}

#[test]
fn callback_read_has_an_overall_deadline_even_when_bytes_keep_arriving() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let sender = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        for _ in 0..20 {
            if stream.write_all(b"x").is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    let (mut stream, _) = listener.accept().unwrap();
    let start = Instant::now();
    assert!(read_callback(&mut stream, start + Duration::from_millis(60)).is_err());
    assert!(start.elapsed() < Duration::from_millis(180));
    drop(stream);
    sender.join().unwrap();
}

#[test]
fn callback_listener_retries_invalid_requests_and_propagates_denial_and_accept_errors() {
    let redirect = "http://127.0.0.1/oauth/callback";
    let mut requests = vec![
        socket_with_request(b"bad\r\n\r\n"),
        socket_with_request(b"GET /other HTTP/1.1\r\n\r\n"),
        socket_with_request(b"GET /oauth/callback?code=abc&state=s HTTP/1.1\r\n\r\n"),
    ]
    .into_iter();
    assert_eq!(
        wait_for_code(
            || Ok(requests.next().unwrap()),
            redirect,
            "s",
            Duration::from_secs(1)
        )
        .unwrap(),
        "abc"
    );
    let mut denied = Some(socket_with_request(
        b"GET /oauth/callback?error=access_denied&state=s HTTP/1.1\r\n\r\n",
    ));
    assert!(wait_for_code(
        || Ok(denied.take().unwrap()),
        redirect,
        "s",
        Duration::from_secs(1)
    )
    .unwrap_err()
    .to_string()
    .contains("denied"));
    assert!(wait_for_code(
        || Err(std::io::ErrorKind::PermissionDenied.into()),
        redirect,
        "s",
        Duration::from_secs(1)
    )
    .is_err());
    assert!(wait_for_code(
        || Err(std::io::ErrorKind::WouldBlock.into()),
        redirect,
        "s",
        Duration::from_millis(30)
    )
    .unwrap_err()
    .to_string()
    .contains("timed out"));
}

#[test]
fn authorization_prompt_handles_json_terminal_and_browser_outcomes() {
    let url = Url::parse("https://api.rawtree.com/oauth/authorize?state=s").unwrap();
    for json_mode in [false, true] {
        for no_browser in [false, true] {
            for browser_succeeded in [false, true] {
                let calls = std::cell::Cell::new(0);
                let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
                announce_authorization(
                    &url,
                    no_browser,
                    300,
                    json_mode,
                    || {
                        calls.set(calls.get() + 1);
                        browser_succeeded
                    },
                    &mut stdout,
                    &mut stderr,
                )
                .unwrap();
                assert_eq!(calls.get(), usize::from(!json_mode && !no_browser));
                if json_mode {
                    assert!(stdout.is_empty());
                    let event: serde_json::Value = serde_json::from_slice(&stderr).unwrap();
                    assert_eq!(event["event"], "oauth_authorization_required");
                    assert_eq!(event["authorization_url"], url.as_str());
                    assert_eq!(event["expires_in"], 300);
                } else {
                    assert!(String::from_utf8(stdout).unwrap().contains(url.as_str()));
                    assert_eq!(stderr.is_empty(), no_browser || browser_succeeded);
                }
            }
        }
    }
}
