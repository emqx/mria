PROJECT = ekka
PROJECT_DESCRIPTION = Autocluster and Autoheal for EMQ
PROJECT_VERSION = 0.2

DEPS = lager
dep_lager = git https://github.com/basho/lager master

LOCAL_DEPS = mnesia

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

CT_SUITES = ekka ekka_lib ekka_autocluster

CT_OPTS = -cover test/ct.cover.spec -erl_args -name ekka_ct@127.0.0.1

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl lager compiler mnesia
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

COVER = true
include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/ekka.conf.example -i priv/ekka.schema -d data/
