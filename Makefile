### BEGIN HEADERS ###
# This block is managed by ServiceBot plugin - Make. The content in this block is created using a common
# template and configurations in service.yml.
# Modifications in this block will be overwritten by generated content in the nightly run.
# For more information, please refer to the page:
# https://confluentinc.atlassian.net/wiki/spaces/Foundations/pages/2871328913/Add+Make
SERVICE_NAME := flink
### END HEADERS ###

UPDATE_MK_INCLUDE := false
UPDATE_MK_INCLUDE_AUTO_MERGE := false
MAVEN_NANO_VERSION := true
include ./mk-include/cc-begin.mk
include ./mk-include/cc-vault.mk
include ./mk-include/cc-maven.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-end.mk

FLINK_VERSION = $(shell ./mvnw org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version | egrep '^[0-9]')

.PHONY: flink-bump-version
flink-bump-version:
ifeq ($(CI),true)
ifneq ($(RELEASE_BRANCH),$(_empty))
	./tools/ci/confluent/bump_nanoversion.sh
else
	@echo "Not release branch - skipping"
endif
else
	@echo "Not on CI - skipping"
endif

.PHONY: flink-deploy
flink-deploy: MAVEN_DEPLOY_REPO_NAME = maven-snapshots
flink-deploy: show-maven
ifeq ($(CI),true)
	@echo "VERSION FOUND: $(FLINK_VERSION)"
ifneq ($(RELEASE_BRANCH),$(_empty))
	./tools/ci/compile_ci.sh || exit $?
	ln -s build-target flink
	tar -chf confluent-flink.tar.gz flink
	make generate-udf-protos
	make mvn-push-nanoversion-tag

	./mvnw -B deploy $(MAVEN_SKIP_CHECKS) \
	   -Prelease,docs-and-source \
	   -Dgpg.skip -Drat.skip \
	   -Dscala-2.12 \
	   -DretryFailedDeploymentCount=10 \
	   -DaltDeploymentRepository=$(MAVEN_DEPLOY_REPO_ID)::default::$(MAVEN_DEPLOY_REPO_URL) \
	   -DrepositoryId=$(MAVEN_DEPLOY_REPO_ID)

	./mvnw -B deploy:deploy-file -DgroupId=io.confluent.flink \
		-DartifactId=flink \
		-Dversion=$(FLINK_VERSION) \
		-Dpackaging=tar.gz \
		-Dfile=confluent-flink.tar.gz \
		-DrepositoryId=$(MAVEN_DEPLOY_REPO_ID) \
		-Durl=$(MAVEN_DEPLOY_REPO_URL)
endif
endif

### BEGIN MK-INCLUDE UPDATE ###
CURL ?= curl
FIND ?= find
TAR ?= tar

# Mount netrc so curl can work from inside a container
DOCKER_NETRC_MOUNT ?= 1

GITHUB_API = api.github.com
GITHUB_MK_INCLUDE_OWNER := confluentinc
GITHUB_MK_INCLUDE_REPO := cc-mk-include
GITHUB_API_CC_MK_INCLUDE := https://$(GITHUB_API)/repos/$(GITHUB_MK_INCLUDE_OWNER)/$(GITHUB_MK_INCLUDE_REPO)
GITHUB_API_CC_MK_INCLUDE_TARBALL := $(GITHUB_API_CC_MK_INCLUDE)/tarball
GITHUB_API_CC_MK_INCLUDE_VERSION ?= $(GITHUB_API_CC_MK_INCLUDE_TARBALL)/$(MK_INCLUDE_VERSION)

MK_INCLUDE_DIR := mk-include
MK_INCLUDE_LOCKFILE := .mk-include-lockfile
MK_INCLUDE_TIMESTAMP_FILE := .mk-include-timestamp
# For optimum performance, you should override MK_INCLUDE_TIMEOUT_MINS above the managed section headers to be
# a little longer than the worst case cold build time for this repo.
MK_INCLUDE_TIMEOUT_MINS ?= 240
# If this latest validated release is breaking you, please file a ticket with DevProd describing the issue, and
# if necessary you can temporarily override MK_INCLUDE_VERSION above the managed section headers until the bad
# release is yanked.
MK_INCLUDE_VERSION ?= v0.937.0

# Make sure we always have a copy of the latest cc-mk-include release less than $(MK_INCLUDE_TIMEOUT_MINS) old:
./$(MK_INCLUDE_DIR)/%.mk: .mk-include-check-FORCE
	@trap "rm -f $(MK_INCLUDE_LOCKFILE); exit" 0 2 3 15; \
	waitlock=0; while ! ( set -o noclobber; echo > $(MK_INCLUDE_LOCKFILE) ); do \
	   sleep $$waitlock; waitlock=`expr $$waitlock + 1`; \
	   test 14 -lt $$waitlock && { \
	      echo 'stealing stale lock after 105s' >&2; \
	      break; \
	   } \
	done; \
	test -s $(MK_INCLUDE_TIMESTAMP_FILE) || rm -f $(MK_INCLUDE_TIMESTAMP_FILE); \
	test -z "`$(FIND) $(MK_INCLUDE_TIMESTAMP_FILE) -mmin +$(MK_INCLUDE_TIMEOUT_MINS) 2>&1`" || { \
	   grep -q 'machine $(GITHUB_API)' ~/.netrc 2>/dev/null || { \
	      echo 'error: follow https://confluentinc.atlassian.net/l/cp/0WXXRLDh to fix your ~/.netrc'; \
	      exit 1; \
	   }; \
	   $(CURL) --fail --silent --netrc --location "$(GITHUB_API_CC_MK_INCLUDE_VERSION)" --output $(MK_INCLUDE_TIMESTAMP_FILE)T --write-out '$(GITHUB_API_CC_MK_INCLUDE_VERSION): %{errormsg}\n' >&2 \
	   && $(TAR) zxf $(MK_INCLUDE_TIMESTAMP_FILE)T \
	   && rm -rf $(MK_INCLUDE_DIR) \
	   && mv $(GITHUB_MK_INCLUDE_OWNER)-$(GITHUB_MK_INCLUDE_REPO)-* $(MK_INCLUDE_DIR) \
	   && mv -f $(MK_INCLUDE_TIMESTAMP_FILE)T $(MK_INCLUDE_TIMESTAMP_FILE) \
	   && echo 'installed cc-mk-include-$(MK_INCLUDE_VERSION) from $(GITHUB_MK_INCLUDE_REPO)' \
	   ; \
	} || { \
	   rm -f $(MK_INCLUDE_TIMESTAMP_FILE)T; \
	   if test -f $(MK_INCLUDE_TIMESTAMP_FILE); then \
	      touch $(MK_INCLUDE_TIMESTAMP_FILE); \
	      echo 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to check for latest release; next try in $(MK_INCLUDE_TIMEOUT_MINS) minutes' >&2; \
	   else \
	      echo 'unable to access $(GITHUB_MK_INCLUDE_REPO) fetch API to bootstrap mk-include subdirectory' >&2 && false; \
	   fi; \
	}

.PHONY: .mk-include-check-FORCE
.mk-include-check-FORCE:
	@test -z "`git ls-files $(MK_INCLUDE_DIR)`" || { \
		echo 'fatal: checked in $(MK_INCLUDE_DIR)/ directory is preventing make from fetching recent cc-mk-include releases for CI' >&2; \
		exit 1; \
	}
### END MK-INCLUDE UPDATE ###


.PHONY: generate-udf-protos
generate-udf-protos:
	protoc cc-flink-extensions/cc-flink-udf-adapter-api/api/v1/*.proto \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --proto_path=.
