# FDK_FILES.md

Every file that belongs to fdk (the Fyers Volume Spike Detector merged from fyersdk), with the classes/functions each one defines. Regenerate with `python scripts/fdk_files_md.py`.

## fdk files

### FDK_FILES.md
- (not python)

### config/fdk_sectors.json
- (not python)

### config/fdk_symbols.json
- (not python)

### docs/fdk_CLAUDE.md
- (not python)

### fdk_env.example
- (not python)

### fdk_main.py
- FdkState
- FdkState.__init__
- supervisor_failure
- fdk_health_check
- _fdk_mcp_asgi
- fdk_mount
- _build_detector
- _register_telegram
- _start_telegram_poller
- _log_supervisor_exit
- _build_mcp
- fdk_lifespan

### fdk_requirements.txt
- (not python)

### fdk_requirements_nodeps.txt
- (not python)

### fdk_tests/conftest.py
- (none)

### fdk_tests/fdk_fakes.py
- FakeCursor
- FakeCursor.__init__
- FakeCursor.__enter__
- FakeCursor.__exit__
- FakeCursor.execute
- FakeCursor.fetchone
- FakeCursor.fetchall
- FakeConn
- FakeConn.__enter__
- FakeConn.__exit__
- FakeConn.cursor
- FakeConn.commit
- FakeConn.close
- FakeResponse
- FakeResponse.__init__
- FakeResponse.json

### fdk_tests/test_auth.py
- (none)

### fdk_tests/test_detector.py
- (none)

### fdk_tests/test_fdk_config_loader.py
- env
- test_shoonya_oauth_inputs_are_loaded
- test_legacy_names_are_accepted_as_fallbacks
- test_client_id_defaults_to_username_with_the_retail_suffix
- test_secret_code_is_required
- test_blank_shoonya_credentials_are_rejected
- test_auto_login_can_be_switched_off
- test_fyers_client_id_is_optional_for_the_dormant_data_socket
- test_removed_variables_are_ignored

### fdk_tests/test_fdk_integration.py
- fresh_state
- posted
- posted._post
- fake_supabase
- idle_orchestrator
- idle_orchestrator._idle
- _make_app
- _mcp
- test_fdk_logger_is_namespaced
- test_disabled_is_inert
- test_enabled_builds_registers_and_serves
- test_polling_mode_when_no_public_url
- test_startup_failure_is_contained
- test_startup_failure_is_contained._down
- test_supervisor_failure_is_read_from_the_task
- test_supervisor_failure_is_read_from_the_task.run
- test_supervisor_failure_is_read_from_the_task.run.boom
- test_supervisor_failure_is_read_from_the_task.run.forever
- test_health_reports_a_dead_supervisor
- test_health_reports_a_dead_supervisor.run
- test_health_reports_a_dead_supervisor.run.boom
- test_missing_env_is_contained
- test_backend_main_is_wired

### fdk_tests/test_fdk_orchestrator.py
- offline
- _orchestrator
- test_builds_the_shoonya_authenticator_and_its_token_table
- test_login_state_tells_waiting_from_failing_from_ok
- test_run_retries_the_initial_supabase_loads_until_they_succeed
- test_run_retries_the_initial_supabase_loads_until_they_succeed.flaky
- test_run_retries_the_initial_supabase_loads_until_they_succeed.drive
- test_shoonya_logs_in_then_idles_without_touching_the_fyers_feed
- test_shoonya_logs_in_then_idles_without_touching_the_fyers_feed.drive

### fdk_tests/test_integrations.py
- (none)

### fdk_tests/test_schedular.py
- _load
- test_should_reset_before_reset_time_is_false
- test_should_reset_at_reset_time_is_true
- test_should_reset_after_reset_time_is_true
- test_should_not_reset_twice_same_day
- test_should_not_reset_in_the_morning
- test_no_rebuild_when_socket_is_healthy
- test_rebuild_immediately_on_first_unhealthy_check
- test_no_rebuild_inside_cooldown
- test_rebuild_again_once_cooldown_elapsed

### fdk_tests/test_sheets.py
- (none)

### fdk_tests/test_shoonya_authenticator.py
- FakeTokenManager
- FakeTokenManager.__init__
- FakeTokenManager.is_token_valid_by_time
- FakeTokenManager.load_token
- FakeTokenManager.save_token
- FakeTokenManager.clear_tokens
- FakeSender
- FakeSender.__init__
- FakeSender.send_async
- _config
- _auth
- _auth._auto
- _auth._exchange
- test_rejects_a_malformed_or_empty_totp_secret_at_construction
- test_reuses_a_cached_session_that_the_broker_still_accepts
- test_automated_login_exchanges_the_captured_code
- test_falls_back_to_the_manual_link_when_the_browser_login_fails
- test_falls_back_to_the_manual_link_when_the_browser_login_fails.run
- test_manual_link_is_used_directly_when_auto_login_is_off
- test_manual_link_is_used_directly_when_auto_login_is_off.run
- test_manual_link_is_used_directly_when_no_browser_is_installed
- test_manual_link_is_used_directly_when_no_browser_is_installed.run
- test_manual_link_is_resent_while_waiting
- test_manual_link_is_resent_while_waiting.run
- test_bad_pasted_code_is_reported_and_the_wait_continues
- test_bad_pasted_code_is_reported_and_the_wait_continues.run
- test_cancel_cuts_the_manual_wait_short
- test_cancel_cuts_the_manual_wait_short.run
- test_reset_clears_the_session_so_the_next_login_is_fresh
- test_reset_cancel_allows_a_new_attempt_after_cancel

### fdk_tests/test_shoonya_login.py
- _load
- _JsonResp
- _JsonResp.__init__
- _HtmlResp
- _HtmlResp.__init__
- _parse_form
- test_oauth_login_url_carries_the_client_id
- test_extract_code_from_a_pasted_redirect_url
- test_checksum_is_sha256_of_client_id_secret_and_code
- test_exchange_code_posts_gen_acs_tok_and_returns_the_session
- test_exchange_code_posts_gen_acs_tok_and_returns_the_session._post
- test_exchange_code_reports_the_brokers_error_message
- test_exchange_code_reports_http_status_when_the_reply_is_not_json
- test_exchange_code_reports_network_failure_instead_of_raising
- test_exchange_code_reports_network_failure_instead_of_raising._boom
- test_validate_session_uses_the_bearer_header
- test_validate_session_uses_the_bearer_header._post
- test_validate_session_rejects_an_expired_token
- test_validate_session_treats_non_json_and_network_errors_as_invalid
- test_validate_session_treats_non_json_and_network_errors_as_invalid._boom
- _FakeElement
- _FakeElement.__init__
- _FakeElement.is_displayed
- _FakeElement.click
- _FakeElement.clear
- _FakeElement.send_keys
- _FakeDriver
- _FakeDriver.__init__
- _FakeDriver.get
- _FakeDriver.find_elements
- _FakeDriver.find_element
- _FakeDriver.get_log
- _FakeDriver.quit
- _auto
- test_auto_login_drives_the_official_login_page_and_captures_the_code
- test_auto_login_retries_once_with_a_fresh_totp_then_gives_up
- test_auto_login_retries_once_with_a_fresh_totp_then_gives_up._sleep
- test_auto_login_reports_a_changed_page_layout
- test_auto_login_reports_a_missing_browser
- test_auto_login_reports_a_missing_browser._no_browser

### fdk_tests/test_summary_scheduler.py
- _Gen
- _Gen.__init__
- _Gen.send_summary
- _run
- test_all_generators_run_when_all_succeed
- test_raising_generator_does_not_block_the_next_one
- test_send_all_never_propagates
- test_generator_returning_false_is_logged

### fdk_tests/test_telegram.py
- sender
- _resp
- test_send_returns_true_on_200
- test_send_returns_false_on_non_200
- test_send_logs_telegram_error_body_on_non_200
- test_send_logs_exception_with_chat_id
- test_send_logs_exception_with_chat_id._boom

### fdk_tests/test_telegram_poller.py
- _Resp
- _Resp.__init__
- _Resp.json
- _drive
- _drive.run
- test_poller_removes_the_webhook_then_dispatches_messages_in_order
- test_poller_removes_the_webhook_then_dispatches_messages_in_order._get
- test_poller_removes_the_webhook_then_dispatches_messages_in_order._post
- test_poller_removes_the_webhook_then_dispatches_messages_in_order.handler
- test_poller_survives_network_errors_and_handler_errors
- test_poller_survives_network_errors_and_handler_errors._get
- test_poller_survives_network_errors_and_handler_errors.handler

### fdk_tests/test_websocket_manager.py
- _Inner
- _Inner.__init__
- _WsObj
- _WsObj.__init__
- FakeSocket
- FakeSocket.__init__
- FakeSocket.is_connected
- FakeSocket.connect
- FakeSocket.subscribe
- FakeSocket.close_connection
- FakeSocket.kill_socket
- FakeSocket.enter_reconnect_window
- manager
- test_initial_connect_subscribes_all_symbols
- test_connect_registers_all_callbacks
- test_reconnect_resubscribes_every_symbol
- test_waits_for_socket_to_open_before_subscribing
- test_does_not_subscribe_if_socket_never_opens
- test_is_alive_true_after_successful_connect
- test_is_alive_false_when_underlying_socket_dropped
- test_needs_rebuild_when_subscription_never_landed
- test_needs_rebuild_when_never_connected
- test_auth_error_invokes_callback
- test_non_auth_error_does_not_invoke_callback
- test_close_stops_reconnect_loop_even_when_close_connection_noops

### scripts/fdk_files_md.py
- functions
- functions.walk
- all_paths
- render

### services/fdk_auth_service/__init__.py
- (none)

### services/fdk_auth_service/server.py
- init_auth_router
- telegram_webhook
- handle_text
- _handle_hold
- _handle_restart
- _handle_send_summary
- _handle_summary_date_prompt
- _handle_summary_date_input

### services/fdk_auth_service/shoonya_authenticator.py
- ShoonyaAuthenticator
- ShoonyaAuthenticator.__init__
- ShoonyaAuthenticator.cancel_auth
- ShoonyaAuthenticator.reset_cancel
- ShoonyaAuthenticator.reset
- ShoonyaAuthenticator.submit_code
- ShoonyaAuthenticator.authenticate
- ShoonyaAuthenticator._manual_login
- ShoonyaAuthenticator._exchange_and_adopt
- ShoonyaAuthenticator._adopt
- ShoonyaAuthenticator._cancellable_sleep

### services/fdk_auth_service/shoonya_login.py
- _sha256
- _totp_now
- _snippet
- _post
- bearer_headers
- oauth_login_url
- extract_code
- checksum
- exchange_code
- validate_session
- auto_login_available
- _chrome_driver
- _scan_network_for_code
- _fill
- auto_login_code

### services/fdk_auth_service/token_manager.py
- TokenManager
- TokenManager.__init__
- TokenManager._get_conn
- TokenManager._ensure_table
- TokenManager.load_token
- TokenManager.save_token
- TokenManager.clear_tokens
- TokenManager.is_token_valid_by_time

### services/fdk_auth_service/tools.py
- register_telegram_webhook
- register_bot_commands
- delete_telegram_webhook

### services/fdk_detector_service/__init__.py
- (none)

### services/fdk_detector_service/detector.py
- VolumeSpikeDetector
- VolumeSpikeDetector.__init__
- VolumeSpikeDetector.on_tick
- VolumeSpikeDetector._process_tick
- VolumeSpikeDetector.start
- VolumeSpikeDetector.stop

### services/fdk_detector_service/models.py
- TickState

### services/fdk_detector_service/server.py
- (none)

### services/fdk_detector_service/tick_handler.py
- parse_tick

### services/fdk_detector_service/tools.py
- (none)

### services/fdk_detector_service/trade_analyzer.py
- analyze_trade

### services/fdk_detector_service/websocket_manager.py
- WebSocketManager
- WebSocketManager.__init__
- WebSocketManager.is_alive
- WebSocketManager.needs_rebuild
- WebSocketManager._wait_until_open
- WebSocketManager._subscribe
- WebSocketManager._on_connect
- WebSocketManager._is_auth_error
- WebSocketManager._on_error
- WebSocketManager._on_close
- WebSocketManager.connect
- WebSocketManager.close
- TickDispatcher
- TickDispatcher.__init__
- TickDispatcher._on_tick
- TickDispatcher._on_auth_error
- TickDispatcher.is_alive
- TickDispatcher.needs_rebuild
- TickDispatcher.connect
- TickDispatcher.close

### services/fdk_fyers_service/__init__.py
- (none)

### services/fdk_fyers_service/service.py
- FyersService
- FyersService.__init__
- FyersService.build
- FyersService.start
- FyersService.stop
- FyersService.detector
- FyersService.is_running
- FyersService.token_expired
- FyersService.update_token

### services/fdk_fyers_service/summary.py
- FyersSummaryService
- FyersSummaryService.__init__
- FyersSummaryService.send_today
- FyersSummaryService.send_for_date

### services/fdk_mcp_service/__init__.py
- (none)

### services/fdk_mcp_service/fyers_tools.py
- _parse_date
- register_fyers_tools
- register_fyers_tools._unavailable
- register_fyers_tools.get_detector_status
- register_fyers_tools.get_volume_summary
- register_fyers_tools.list_monitored_symbols
- register_fyers_tools.get_sector_mapping

### services/fdk_mcp_service/news_tools.py
- _fetch_feed_sync
- _extract_article_sync
- _format_telegram_news
- register_news_tools
- register_news_tools.fetch_news_headlines
- register_news_tools.fetch_article_content
- register_news_tools.list_news_feeds
- register_news_tools.send_news_to_telegram

### services/fdk_mcp_service/server.py
- mcp_path
- _news_sender
- build_mcp

### services/fdk_penny_service/__init__.py
- (none)

### services/fdk_penny_service/service.py
- PennyService
- PennyService.__init__
- PennyService.build
- PennyService.start
- PennyService.stop
- PennyService.detector
- PennyService.is_running
- PennyService.token_expired
- PennyService.update_token

### services/fdk_penny_service/summary.py
- PennySummaryService
- PennySummaryService.__init__
- PennySummaryService.send_today
- PennySummaryService.send_for_date

### services/fdk_sector_service/__init__.py
- (none)

### services/fdk_sector_service/models.py
- SectorInfo

### services/fdk_sector_service/sector_mapper.py
- init_sector_mapping
- get_sector

### services/fdk_sector_service/server.py
- (none)

### services/fdk_sector_service/symbol_manager.py
- SymbolManager
- SymbolManager.__init__
- SymbolManager._get_conn
- SymbolManager._ensure_tables
- SymbolManager._seed_if_empty
- SymbolManager._seed_symbols_from_json
- SymbolManager._seed_sectors_from_json
- SymbolManager.load_symbols
- SymbolManager.load_sector_mapping
- SymbolManager.seed_symbols
- SymbolManager.seed_sectors

### services/fdk_sector_service/tools.py
- get_all_sectors
- get_symbols_for_sector

### services/fdk_sheets_service/__init__.py
- (none)

### services/fdk_sheets_service/models.py
- SheetRow

### services/fdk_sheets_service/row_builder.py
- build_row
- row_to_list

### services/fdk_sheets_service/server.py
- (none)

### services/fdk_sheets_service/sheet_manager.py
- GoogleSheetsManager
- GoogleSheetsManager.__init__
- GoogleSheetsManager._initialize
- GoogleSheetsManager.add_trade

### services/fdk_sheets_service/tools.py
- (none)

### services/fdk_summary_service/__init__.py
- (none)

### services/fdk_summary_service/summary_generator.py
- SummaryGenerator
- SummaryGenerator.__init__
- SummaryGenerator._init_sheets
- SummaryGenerator._find_columns
- SummaryGenerator._date_formats
- SummaryGenerator._get_records
- SummaryGenerator._aggregate
- SummaryGenerator._format_message
- SummaryGenerator._generate_messages_for
- SummaryGenerator.generate_messages
- SummaryGenerator.generate_messages_for_date
- SummaryGenerator.send_summary
- SummaryGenerator.send_summary_for_date
- SummaryGenerator._send

### services/fdk_summary_service/summary_scheduler.py
- SummaryScheduler
- SummaryScheduler.__init__
- SummaryScheduler._send_all
- SummaryScheduler.run

### services/fdk_supervisor_service/__init__.py
- (none)

### services/fdk_supervisor_service/health_monitor.py
- start_self_ping
- start_self_ping._ping_loop

### services/fdk_supervisor_service/models.py
- DetectorState

### services/fdk_supervisor_service/ochestrator.py
- Orchestrator
- Orchestrator.__init__
- Orchestrator.hold
- Orchestrator.hold_async
- Orchestrator.login_state
- Orchestrator.request_restart
- Orchestrator._any_token_expired
- Orchestrator._close_dispatcher
- Orchestrator._close_dispatcher_async
- Orchestrator._connect_dispatcher
- Orchestrator._daily_token_reset
- Orchestrator._re_authenticate
- Orchestrator._authenticate_with_checks
- Orchestrator.run

### services/fdk_supervisor_service/run_controller.py
- RunController
- RunController.__init__
- RunController.is_running
- RunController.start
- RunController.stop
- RunController._worker

### services/fdk_supervisor_service/schedular.py
- is_market_hours
- should_reset_tokens
- should_rebuild_socket

### services/fdk_supervisor_service/server.py
- (none)

### services/fdk_supervisor_service/tools.py
- (none)

### services/fdk_telegram_service/README.md
- (not python)

### services/fdk_telegram_service/__init__.py
- (none)

### services/fdk_telegram_service/bot_handler.py
- TelegramSender
- TelegramSender.__init__
- TelegramSender.send
- TelegramSender.send_async

### services/fdk_telegram_service/command_parser.py
- parse_date
- parse_command

### services/fdk_telegram_service/message_template.py
- trade_alert_message
- shoonya_login_link_message
- auth_success_message
- auth_failure_message
- hold_message
- restart_message
- restart_complete_message
- monitoring_started_message
- summary_generating_message
- summary_date_prompt_message
- summary_date_invalid_message
- summary_date_confirm_message
- websocket_rebuild_message

### services/fdk_telegram_service/models.py
- TelegramMessage

### services/fdk_telegram_service/poller.py
- TelegramPoller
- TelegramPoller.__init__
- TelegramPoller.run
- TelegramPoller._delete_webhook
- TelegramPoller._get_updates

### services/fdk_telegram_service/server.py
- (none)

### services/fdk_telegram_service/tools.py
- (none)

### shared/fdk_config_loader.py
- fdk_enabled
- ShoonyaConfig
- TelegramChannel
- TelegramConfig
- GoogleConfig
- SupabaseConfig
- AppConfig
- _required
- _first_present
- _shoonya_config
- _channel
- _optional_channel
- load_config
- _load_google_credentials

### shared/fdk_constants.py
- _flag

### shared/fdk_exceptions.py
- AuthenticationError
- TokenExpiredError
- WebSocketError
- SupabaseError

### shared/fdk_logger.py
- ISTFormatter
- ISTFormatter.formatTime
- get_logger

### shared/fdk_models.py
- TickData
- TradeAlert
- DetectorConfig

## Touch points in existing files (lines marked `fdk`)

- main.py
- requirements.txt
- Dockerfile
