.PHONY: tests

PROJECT = ekka
PROJECT_DESCRIPTION = Autocluster and Autoheal for EMQ X Broker
PROJECT_VERSION = 0.4.1

DEPS = jsx
dep_jsx = git https://github.com/talentdeficit/jsx 2.9.0

BUILD_DEPS = lager
dep_lager = git https://github.com/erlang-lager/lager 3.6.4

LOCAL_DEPS = mnesia inets

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqx/cuttlefish

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

CT_SUITES = ekka ekka_lib ekka_autocluster ekka_locker

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
