PROJECT = ekka
PROJECT_DESCRIPTION = A New Distribution Layer for EMQ X
PROJECT_VERSION = 0.1

LOCAL_DEPS = mnesia

NO_AUTOPATCH = cuttlefish

BUILD_DEPS = cuttlefish
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

include erlang.mk

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/ekka.conf -i priv/ekka.schema -d data/
