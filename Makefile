PROJECT = ekka
PROJECT_DESCRIPTION = Autocluster and Autoheal for EMQ
PROJECT_VERSION = 0.1

DEPS = lager
dep_lager = git https://github.com/basho/lager master

LOCAL_DEPS = mnesia

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/ekka.conf -i priv/ekka.schema -d data/

