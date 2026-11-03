# FDK_FILES.md

Every file that belongs to fdk (the Shoonya volume spike detector merged from fyersdk), with the classes/functions each one defines. Regenerate with `python scripts/fdk_files_md.py`.

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

### fdk_tests/conftest.py
- no_real_sockets
- no_real_sockets.refuse

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
- wait_for
- alerts
- _build
- detector
- _tick
- test_parse_reads_shoonya_fields_sent_as_strings
- test_parse_rejects_what_cannot_be_a_trade
- test_parse_only_accepts_tick_frames
- test_the_first_snapshot_is_a_baseline_not_a_trade
- test_a_diff_above_the_threshold_alerts_with_the_traded_value
- test_a_diff_below_the_threshold_stays_quiet
- test_volume_accrued_during_an_outage_is_not_reported_as_one_trade
- test_the_first_diff_after_a_baseless_start_is_a_baseline_too
- test_alerts_are_throttled_per_symbol
- test_a_snapshot_saying_nothing_has_traded_yet_is_a_baseline_of_zero
- test_a_snapshot_with_no_volume_field_is_still_not_treated_as_zero
- test_a_hung_sheet_write_does_not_hold_up_tick_processing
- test_alerts_raised_before_start_go_out_once_it_runs
- test_queued_alerts_still_go_out_when_stopping
- test_a_full_alert_queue_loses_the_alert_loudly
- test_a_delivery_failure_does_not_end_the_alert_thread
- test_a_delivery_failure_does_not_end_the_alert_thread.add_trade
- test_sheet_writes_have_a_timeout
- test_summaries_count_old_and_new_names_as_one_stock

### fdk_tests/test_fdk_config_loader.py
- env
- test_shoonya_oauth_inputs_are_loaded
- test_legacy_names_are_accepted_as_fallbacks
- test_client_id_defaults_to_username_with_the_retail_suffix
- test_webhook_secret_is_optional
- test_webhook_secret_must_use_telegrams_charset
- test_bot_and_sheet_settings_use_shoonya_names
- test_old_fyers_bot_and_sheet_names_still_work_for_one_release
- test_a_missing_bot_setting_is_named_by_its_new_name
- test_secret_code_is_required
- test_blank_shoonya_credentials_are_rejected
- test_registered_redirect_is_kept_as_a_hint_for_the_browser_login
- test_auto_login_can_be_switched_off
- test_fyers_client_id_went_with_the_fyers_socket
- test_removed_variables_are_ignored

### fdk_tests/test_fdk_integration.py
- fresh_state
- _reset_server_globals
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
- test_webhook_secret_is_registered_and_enforced
- test_polling_mode_when_no_public_url
- test_polling_handler_carries_the_login_chat
- test_polling_handler_carries_the_login_chat._CapturingPoller
- test_polling_handler_carries_the_login_chat._CapturingPoller.__init__
- test_polling_handler_carries_the_login_chat._CapturingPoller.run
- test_failed_webhook_registration_is_reported
- test_failed_webhook_registration_is_reported._post
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
- test_health_shows_the_feed_and_shutdown_closes_it
- test_health_shows_the_feed_and_shutdown_closes_it.Dispatcher
- test_health_shows_the_feed_and_shutdown_closes_it.Dispatcher.status
- test_health_shows_the_feed_and_shutdown_closes_it.Dispatcher.close
- test_backend_main_is_wired

### fdk_tests/test_fdk_orchestrator.py
- offline
- _orchestrator
- test_builds_the_shoonya_authenticator_and_its_token_table
- test_login_state_tells_waiting_from_failing_from_ok
- test_login_state_reads_parked_after_a_failed_day
- test_manual_login_wait_follows_market_hours
- test_run_retries_the_initial_supabase_loads_until_they_succeed
- test_run_retries_the_initial_supabase_loads_until_they_succeed.flaky
- test_run_retries_the_initial_supabase_loads_until_they_succeed.drive
- FakeDispatcher
- FakeDispatcher.reset
- FakeDispatcher.__init__
- FakeDispatcher.connect
- FakeDispatcher.close
- FakeDispatcher.needs_rebuild
- FakeDispatcher.is_up
- FakeDispatcher.ticks_seen
- FakeDispatcher.ready
- FakeDispatcher.monitored
- FakeDispatcher.take_coverage_report
- FakeDispatcher.status
- _detector
- feed
- feed.load_symbols
- feed.build
- feed.build._build
- feed.record_message
- _drive
- _drive.drive
- _connected
- _sent
- _set
- _after
- _after.elapsed
- test_after_login_the_shoonya_feed_starts_with_user_id_and_tokens
- test_monitoring_started_names_shoonya_and_the_counts
- test_a_feed_that_is_not_up_says_so_once_and_announces_when_it_recovers
- test_a_long_outage_alerts_once_not_on_every_rebuild
- test_a_long_outage_alerts_once_not_on_every_rebuild.go_down
- test_a_long_outage_alerts_once_not_on_every_rebuild.come_back
- test_monitoring_started_waits_for_the_first_tick_during_trading
- test_before_the_first_trades_monitoring_started_does_not_wait_for_ticks
- test_a_rebuilt_feed_that_stays_silent_is_alerted_again_until_it_ticks
- test_a_rebuilt_feed_that_stays_silent_is_alerted_again_until_it_ticks.go_down
- test_a_rebuilt_feed_that_stays_silent_is_alerted_again_until_it_ticks.back_but_silent
- test_the_daily_refresh_runs_before_the_universe_is_loaded_and_reloads_on_renames
- test_the_daily_refresh_runs_before_the_universe_is_loaded_and_reloads_on_renames.once
- test_the_daily_refresh_runs_before_the_universe_is_loaded_and_reloads_on_renames.refresh
- test_the_daily_refresh_runs_before_the_universe_is_loaded_and_reloads_on_renames.load_symbols
- test_a_refresh_that_changes_names_while_running_rebuilds_the_detectors
- test_a_refresh_that_changes_names_while_running_rebuilds_the_detectors.when_told
- test_a_refresh_that_changes_names_while_running_rebuilds_the_detectors.trigger
- test_a_failed_refresh_is_reported_and_does_not_stop_the_day
- test_a_failed_refresh_is_reported_and_does_not_stop_the_day.once
- test_a_failed_refresh_is_reported_and_does_not_stop_the_day.boom
- _report
- test_symbols_the_gateway_did_not_acknowledge_are_named_with_their_detector
- test_the_same_gap_is_reported_once_and_a_new_one_again
- test_full_coverage_sends_no_warning
- test_symbols_without_a_token_are_not_counted_as_monitored
- stored_session
- stored_session.clear
- test_a_socket_rejection_discards_the_session_and_logs_in_fresh
- test_a_socket_rejection_discards_the_session_and_logs_in_fresh.recover
- test_a_fresh_session_also_rejected_stops_retrying_and_says_why
- test_rst_gets_a_parked_feed_going_again
- test_rst_gets_a_parked_feed_going_again.operator_fixes_it_and_sends_rst
- test_a_healthy_feed_earns_a_fresh_rejection_budget
- test_a_healthy_feed_earns_a_fresh_rejection_budget.reject
- test_a_healthy_feed_earns_a_fresh_rejection_budget.recover
- test_a_database_blip_during_a_rejection_does_not_use_up_the_retry
- test_a_database_blip_during_a_rejection_does_not_use_up_the_retry.flaky_clear
- test_a_database_blip_during_a_rejection_does_not_use_up_the_retry.recover
- _longest_stall
- _longest_stall.main
- _longest_stall.main.heartbeat
- test_stopping_the_detectors_does_not_freeze_the_event_loop
- test_stopping_the_detectors_does_not_freeze_the_event_loop.no_login
- test_the_supervisor_loop_stops_detectors_off_the_event_loop
- test_hold_during_the_close_stops_the_relogin
- test_hold_during_the_close_stops_the_relogin.close_while_hld_arrives
- test_market_close_closes_the_feed_even_if_the_detectors_are_not_running
- test_feed_status_with_and_without_a_feed
- test_summaries_are_titled_with_the_database_label
- test_the_large_cap_detector_config_is_labelled_shoonya
- test_token_manager_defaults_to_the_only_token_table
- test_load_instrument_tokens_maps_active_symbols_through_isin
- test_load_instrument_tokens_maps_active_symbols_through_isin.Cursor
- test_load_instrument_tokens_maps_active_symbols_through_isin.Cursor.execute
- test_load_instrument_tokens_maps_active_symbols_through_isin.Cursor.fetchall
- test_load_instrument_tokens_maps_active_symbols_through_isin.Conn
- test_load_instrument_tokens_maps_active_symbols_through_isin.Conn.cursor
- _FakeMCP
- _FakeMCP.__init__
- _FakeMCP.tool
- _FakeMCP.tool.register
- mcp_tools
- test_detector_status_shows_the_feed
- test_listing_symbols_defaults_to_the_shoonya_label
- test_the_fyers_label_is_no_longer_accepted
- test_operator_messages_do_not_name_the_old_broker
- test_the_fyers_name_is_gone_from_the_current_code
- test_health_and_status_report_shoonya_running
- test_nothing_imports_the_fyers_sdk
- test_the_fyers_sdk_is_not_installed_by_the_image

### fdk_tests/test_feed_real_socket.py
- LocalGateway
- LocalGateway.__init__
- LocalGateway._handle
- LocalGateway.push
- LocalGateway.drop_client
- LocalGateway.stop
- wait_for
- Detector
- Detector.__init__
- Detector.on_tick
- gateway
- feed
- test_handshake_batches_and_ticks_over_a_real_socket
- test_a_real_drop_reconnects_and_resubscribes_everything
- test_an_idle_socket_outlives_the_socket_timeout
- test_a_gateway_that_accepts_and_never_answers_is_retried_then_handed_over
- test_a_gateway_that_accepts_and_never_answers_is_retried_then_handed_over.accept_and_stay_silent
- test_a_real_rejection_flags_the_token_and_stops_dialling
- test_close_ends_the_real_socket_and_threads

### fdk_tests/test_instrument_refresh.py
- _master_text
- _master
- _stored
- test_parse_master_maps_token_to_trading_symbol_and_series
- test_parse_master_rejects_a_file_with_the_wrong_columns
- test_parse_master_skips_short_rows
- test_download_master_unzips_and_parses
- test_download_master_unzips_and_parses.Resp
- test_download_master_unzips_and_parses.Resp.raise_for_status
- test_download_master_unzips_and_parses._get
- _plan
- test_unchanged_instruments_are_verified_and_nothing_else
- test_a_series_move_is_renamed_automatically
- test_a_name_change_is_reported_not_applied
- test_a_series_move_onto_a_symbol_already_in_use_is_reported_not_applied
- test_a_token_missing_from_the_file_is_reported
- test_too_many_missing_tokens_aborts_the_refresh
- test_a_file_with_too_few_rows_aborts_the_refresh
- _Cursor
- _Cursor.fetchall
- _Conn
- _Conn.__init__
- _Conn.cursor
- _Conn.commit
- _Conn.rollback
- test_apply_renames_the_three_tables_and_stamps_verified_on_in_one_transaction
- test_apply_writes_nothing_for_an_aborted_plan
- test_apply_rolls_back_when_a_statement_fails
- test_apply_rolls_back_when_a_statement_fails.boom
- test_refresher_loads_stored_rows_plans_and_applies
- test_refresher_loads_stored_rows_plans_and_applies.connect
- test_refresh_message_lists_renames_decisions_and_missing
- test_refresh_message_for_an_aborted_refresh_says_why

### fdk_tests/test_integrations.py
- (none)

### fdk_tests/test_schedular.py
- _load
- test_should_reset_before_reset_time_is_false
- test_should_reset_at_reset_time_is_true
- test_should_reset_after_reset_time_is_true
- test_should_not_reset_twice_same_day
- test_should_not_reset_in_the_morning
- test_instruments_refresh_once_a_day_from_0830
- test_ticks_are_due_between_the_first_trades_and_the_close
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
- test_browser_login_is_tried_twice_by_default_and_passes_the_redirect_hint
- test_browser_login_is_tried_twice_by_default_and_passes_the_redirect_hint._auto
- test_browser_login_is_tried_twice_by_default_and_passes_the_redirect_hint.run
- test_fatal_browser_error_skips_straight_to_the_manual_link
- test_fatal_browser_error_skips_straight_to_the_manual_link.run
- test_manual_wait_yields_to_the_supervisor_when_told_to_stop_waiting
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
- _FakeDriver._current_inputs
- _FakeDriver.find_elements
- _FakeDriver.find_element
- _FakeDriver.get_log
- _FakeDriver.quit
- _stale_guard
- _stale_guard.wrapper
- _auto
- test_auto_login_drives_the_official_login_page_and_captures_the_code
- test_auto_login_resubmits_exactly_once_then_gives_up
- test_auto_login_resubmits_exactly_once_then_gives_up._totp
- test_auto_login_resubmits_exactly_once_then_gives_up._sleep
- test_auto_login_stops_when_cancelled
- test_auto_login_stops_when_cancelled._cancelled
- test_auto_login_caps_the_page_load_time
- test_auto_login_accepts_a_long_code_on_a_non_shoonya_redirect
- test_auto_login_accepts_the_registered_redirect_even_with_a_short_code
- test_auto_login_ignores_short_code_params_on_foreign_urls
- test_auto_login_ignores_short_code_params_on_foreign_urls._sleep
- test_totp_resubmit_requeries_the_inputs_after_the_page_rerenders
- test_totp_resubmit_requeries_the_inputs_after_the_page_rerenders._sleep
- test_failure_reports_the_page_url_and_visible_text
- test_failure_reports_the_page_url_and_visible_text._sleep
- test_failure_redacts_the_code_and_points_at_the_redirect_setting
- test_failure_redacts_the_code_and_points_at_the_redirect_setting._sleep
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

### fdk_tests/test_symbol_manager.py
- _Cursor
- _Cursor.execute
- _Cursor.fetchone
- _Conn
- _Conn.cursor
- db
- _sql
- _index
- test_tables_are_created_with_instruments_first_and_isin_on_stock_symbols
- test_symbols_with_no_isin_are_linked_to_instruments_by_name
- test_an_empty_database_is_seeded_instruments_first_with_isins
- test_a_seeded_database_is_left_alone
- test_a_missing_instruments_file_is_a_warning_not_a_crash
- test_the_shipped_seed_files_use_shoonya_names
- test_export_seed_writes_the_three_files_from_the_database
- test_export_seed_writes_the_three_files_from_the_database.Cursor
- test_export_seed_writes_the_three_files_from_the_database.Cursor.fetchall
- test_export_seed_writes_the_three_files_from_the_database.Conn
- test_export_seed_writes_the_three_files_from_the_database.Conn.cursor

### fdk_tests/test_telegram.py
- sender
- _resp
- test_send_returns_true_on_200
- test_send_returns_false_on_non_200
- test_send_logs_telegram_error_body_on_non_200
- test_send_logs_exception_with_chat_id
- test_send_logs_exception_with_chat_id._boom
- _alert
- test_trade_alert_escapes_symbol_and_sector_for_telegram_html
- test_summary_escapes_symbols_for_telegram_html

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
- test_poller_only_listens_to_the_login_chat
- test_poller_only_listens_to_the_login_chat._get
- test_poller_only_listens_to_the_login_chat.handler
- test_poller_survives_network_errors_and_handler_errors
- test_poller_survives_network_errors_and_handler_errors._get
- test_poller_survives_network_errors_and_handler_errors.handler

### fdk_tests/test_websocket_manager.py
- FakeApp
- FakeApp.reset
- FakeApp.__init__
- FakeApp.run_forever
- FakeApp.send
- FakeApp.close
- FakeApp.push
- FakeApp.drop
- FakeApp.subscribe_frames
- wait_for
- gateway
- build
- build._make
- test_connect_frame_is_the_documented_one_and_goes_first
- test_heartbeat_matches_the_vendor_sdk
- test_nothing_is_subscribed_before_the_connect_ack
- test_every_documented_spelling_of_the_ack_counts
- test_rejected_connect_flags_auth_and_stops_retrying
- test_scrips_go_out_in_batches_joined_with_hash
- test_batch_limits_are_the_agreed_ones
- test_batches_pause_between_each_other_but_not_after_the_last
- test_connect_returns_only_after_every_scrip_is_acknowledged
- test_partial_acknowledgement_is_reported_not_retried
- test_full_acknowledgement_reports_no_gap
- test_coverage_is_rechecked_after_every_reconnect
- test_no_acknowledgement_at_all_needs_a_rebuild
- test_a_drop_reconnects_and_replays_connect_and_every_batch
- test_backoff_doubles_up_to_the_cap
- test_retrying_is_not_yet_a_rebuild_but_giving_up_is
- test_a_stable_connection_earns_a_fresh_retry_budget
- test_attempts_that_never_opened_earn_no_fresh_budget
- test_close_wakes_a_connect_that_is_still_waiting
- test_close_during_a_backoff_wait_ends_the_loop
- test_close_closes_the_open_socket
- test_a_socket_that_opens_after_close_is_closed_straight_away
- test_never_connected_needs_a_rebuild
- test_alive_while_the_socket_is_up
- test_is_up_means_an_open_socket_with_acknowledged_scrips
- test_is_up_is_false_for_a_socket_that_acknowledged_nothing
- test_a_reconnected_socket_that_stays_quiet_counts_as_stalled
- test_ticks_going_quiet_during_the_session_needs_a_rebuild
- test_quiet_after_the_close_is_not_a_stall
- test_a_rebuilt_feed_that_stays_quiet_counts_as_stalled
- test_ticks_seen_turns_true_with_the_first_diff
- test_no_ticks_ever_is_not_a_stall
- test_ticks_expected_only_until_the_close_ist
- test_garbage_and_foreign_frames_do_not_reach_the_tick_callback
- test_frames_nobody_documented_are_logged_once_so_a_live_run_can_show_them
- FakeDetector
- FakeDetector.__init__
- FakeDetector.on_tick
- dispatch
- dispatch._make
- test_a_symbol_two_detectors_share_is_subscribed_once
- test_ticks_reach_only_the_detectors_that_own_the_symbol
- test_a_diff_is_merged_onto_the_snapshot
- test_a_failing_detector_does_not_cost_the_other_one_the_tick
- test_a_failing_detector_does_not_cost_the_other_one_the_tick.Broken
- test_a_failing_detector_does_not_cost_the_other_one_the_tick.Broken.on_tick
- test_a_slow_detector_does_not_block_the_read_loop
- test_a_slow_detector_does_not_block_the_read_loop.Slow
- test_a_slow_detector_does_not_block_the_read_loop.Slow.on_tick
- test_an_overflowing_queue_drops_ticks_and_keeps_running
- test_an_overflowing_queue_drops_ticks_and_keeps_running.Slow
- test_an_overflowing_queue_drops_ticks_and_keeps_running.Slow.on_tick
- test_a_token_nobody_owns_is_ignored
- test_symbols_without_a_token_are_skipped_and_named
- test_a_snapshot_under_another_name_is_flagged
- test_a_rejected_token_marks_every_detector_expired
- _HeldDetector
- _HeldDetector.on_tick
- _held_detector
- test_ticks_lost_to_a_full_queue_are_not_reported_as_one_big_trade
- test_a_snapshot_lost_to_a_full_queue_cannot_alert_the_outage_volume
- test_a_slow_sheet_write_does_not_cost_any_ticks
- test_missing_symbols_are_named_and_split_by_detector
- test_monitored_counts_only_symbols_with_a_token
- test_nothing_to_subscribe_opens_no_socket
- test_a_dispatcher_expecting_ticks_is_ready_only_once_one_arrives
- test_a_dispatcher_not_expecting_ticks_is_ready_when_subscribed
- test_status_reports_the_feed
- test_a_failing_socket_close_still_stops_the_worker
- test_a_failing_socket_close_still_stops_the_worker.boom
- test_coverage_counts_scrips_not_symbols
- test_closing_the_dispatcher_stops_its_threads

### migrations/2026-11-02_shoonya_only.sql
- (not python)

### scripts/fdk_export_seed.py
- export_seed
- main

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
- _handle_pasted_code
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
- _visible_inputs
- _page_diagnostics
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
- VolumeSpikeDetector._deliver
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
- _backoff_delay
- _pause
- _ticks_expected
- WebSocketManager
- WebSocketManager.__init__
- WebSocketManager.is_alive
- WebSocketManager.is_up
- WebSocketManager.ticks_seen
- WebSocketManager.needs_rebuild
- WebSocketManager._stalled
- WebSocketManager.coverage
- WebSocketManager.seconds_since_tick
- WebSocketManager.connect
- WebSocketManager.close
- WebSocketManager._run
- WebSocketManager._on_open
- WebSocketManager._on_message
- WebSocketManager._log_unexpected
- WebSocketManager._on_tick_frame
- WebSocketManager._on_connect_ack
- WebSocketManager._subscribe
- WebSocketManager._report_coverage
- WebSocketManager._on_error
- WebSocketManager._on_close
- TickDispatcher
- TickDispatcher.__init__
- TickDispatcher._on_frame
- TickDispatcher._drain
- TickDispatcher._on_auth_error
- TickDispatcher._on_coverage
- TickDispatcher.take_coverage_report
- TickDispatcher.monitored
- TickDispatcher.rejection_reason
- TickDispatcher.is_alive
- TickDispatcher.is_up
- TickDispatcher.ticks_seen
- TickDispatcher.ready
- TickDispatcher.needs_rebuild
- TickDispatcher.coverage
- TickDispatcher.status
- TickDispatcher.connect
- TickDispatcher.close

### services/fdk_mcp_service/__init__.py
- (none)

### services/fdk_mcp_service/detector_tools.py
- _parse_date
- register_detector_tools
- register_detector_tools._unavailable
- register_detector_tools.get_detector_status
- register_detector_tools.get_volume_summary
- register_detector_tools.list_monitored_symbols
- register_detector_tools.get_sector_mapping

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

### services/fdk_sector_service/instrument_refresh.py
- parse_master
- download_master
- RefreshPlan
- RefreshPlan.changed
- RefreshPlan.summary
- plan_refresh
- apply_plan
- InstrumentRefresher
- InstrumentRefresher.__init__
- InstrumentRefresher.run

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
- SymbolManager._link_symbols_to_instruments
- SymbolManager._seed_instruments_from_json
- SymbolManager._seed_symbols_from_json
- SymbolManager._seed_sectors_from_json
- SymbolManager.load_symbols
- SymbolManager.load_instrument_tokens
- SymbolManager.load_sector_mapping
- SymbolManager.seed_instruments
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

### services/fdk_shoonya_service/__init__.py
- (none)

### services/fdk_shoonya_service/service.py
- ShoonyaService
- ShoonyaService.__init__
- ShoonyaService.build
- ShoonyaService.start
- ShoonyaService.stop
- ShoonyaService.detector
- ShoonyaService.is_running
- ShoonyaService.token_expired
- ShoonyaService.update_token

### services/fdk_shoonya_service/summary.py
- ShoonyaSummaryService
- ShoonyaSummaryService.__init__
- ShoonyaSummaryService.send_today
- ShoonyaSummaryService.send_for_date

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
- Orchestrator._stop_detectors
- Orchestrator._login_wait_allowed
- Orchestrator.login_state
- Orchestrator.request_restart
- Orchestrator._any_token_expired
- Orchestrator.feed_status
- Orchestrator._forget_feed
- Orchestrator._close_dispatcher
- Orchestrator._close_dispatcher_async
- Orchestrator._connect_dispatcher
- Orchestrator._report_feed_state
- Orchestrator._remind_if_silent
- Orchestrator._on_feed_rejected
- Orchestrator._daily_token_reset
- Orchestrator._re_authenticate
- Orchestrator._authenticate_with_checks
- Orchestrator._load_universe
- Orchestrator._maybe_refresh_instruments
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
- feed_should_be_live
- should_reset_tokens
- should_refresh_instruments
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
- redact_code
- parse_date
- parse_command

### services/fdk_telegram_service/message_template.py
- trade_alert_message
- shoonya_login_link_message
- shoonya_login_paused_message
- auth_success_message
- auth_failure_message
- hold_message
- restart_message
- restart_complete_message
- monitoring_started_message
- feed_coverage_warning_message
- feed_not_up_message
- feed_rejected_message
- summary_generating_message
- summary_date_prompt_message
- summary_date_invalid_message
- summary_date_confirm_message
- instrument_refresh_message
- instrument_refresh_failed_message
- feed_still_silent_message
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
- _channel.read
- _webhook_secret
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
