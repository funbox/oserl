{application, oserl, [
    {description, "Open SMPP Erlang Library"},
    {vsn, "5.1.0"}, % version 4 already used.
    {modules, [
        gen_esme_session,
        gen_esme,
        gen_mc_session,
        gen_mc,
        proxy_protocol,
        time,
        smpp_base,
        smpp_base_syntax,
        smpp_disk_log_hlr,
        smpp_error,
        smpp_log_mgr,
        smpp_operation,
        smpp_param_syntax,
        smpp_pdu_syntax,
        smpp_ref_num,
        smpp_req_tab,
        smpp_session,
        smpp_sm,
        smpp_tty_log_hlr,
        oserl_config
    ]},
    {registered, []},
    {applications, [kernel, stdlib, common_lib]},
    {env, []}
]}.
