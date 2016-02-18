%% -*- mode: erlang -*-

{config, "test.conf"}.
{cover, "cover.conf"}.
{alias, test, ".."}.
{suites, test, all}.
%{skip_suites, test, [gen_mc_SUITE], "not implemented"}.
{skip_cases, test, gen_mc_SUITE, [outbind, errors], "not implemented"}.
