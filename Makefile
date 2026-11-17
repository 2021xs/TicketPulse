.PHONY: setup migrate run test integration build vet smoke smoke-actions smoke-lifecycle smoke-collaboration smoke-session browser stop-mysql search-setup search-start sync run-search search-status integration-search smoke-search browser-search

# Historical artifacts can contain incomplete Go source snapshots. Build only live packages.
GO_PACKAGES := ./cmd/... ./internal/... ./db ./web ./scripts/demo_launcher

setup:
	python3 scripts/local_mysql.py setup

migrate:
	python3 scripts/local_mysql.py migrate

run:
	python3 scripts/local_mysql.py run

test:
	go test $(GO_PACKAGES)

integration:
	python3 scripts/local_mysql.py test

build:
	go build -o bin/ticketpulse ./cmd/ticketpulse
	go build -o bin/ticketpulse-sync ./cmd/ticketpulse-sync
	go build -o bin/ticketpulse-rebuild ./cmd/ticketpulse-rebuild
	go build -o bin/ticketpulse-control ./cmd/ticketpulse-control

vet:
	go vet $(GO_PACKAGES)

smoke:
	python3 scripts/smoke.py

smoke-actions:
	python3 scripts/smoke_actions.py

smoke-lifecycle:
	python3 scripts/smoke_lifecycle.py

smoke-collaboration:
	python3 scripts/smoke_collaboration.py

smoke-session:
	python3 scripts/smoke_session.py

browser: build
	python3 scripts/browser_acceptance.py

stop-mysql:
	python3 scripts/local_mysql.py stop

search-setup:
	python3 scripts/local_kafka.py setup

search-start:
	python3 scripts/local_kafka.py start

sync:
	python3 scripts/local_kafka.py sync

run-search:
	python3 scripts/local_kafka.py web

search-status:
	python3 scripts/local_kafka.py status

integration-search:
	python3 scripts/search_integration.py

smoke-search: build
	python3 scripts/search_smoke.py

browser-search: build
	python3 scripts/search_browser.py

.PHONY: kafka-setup kafka-start kafka-status kafka-migrate analytics-setup run-analytics sync-analytics analytics-status integration-analytics smoke-analytics browser-analytics
kafka-setup:
	python3 scripts/local_kafka.py setup
kafka-start:
	python3 scripts/local_kafka.py start
kafka-status:
	python3 scripts/local_kafka.py status
kafka-migrate: build
	python3 scripts/local_kafka.py migrate
analytics-setup:
	python3 scripts/local_analytics.py setup
run-analytics:
	python3 scripts/local_analytics.py web
sync-analytics:
	python3 scripts/local_analytics.py sync
analytics-status:
	python3 scripts/local_analytics.py status
integration-analytics:
	python3 scripts/analytics_integration.py
smoke-analytics: build
	python3 scripts/analytics_smoke.py
browser-analytics: build
	python3 scripts/analytics_browser.py

.PHONY: rebuild-migrate rebuild-check integration-rebuild
rebuild-migrate: build
	python3 scripts/local_rebuild.py migrate
rebuild-check: build
	python3 scripts/local_rebuild.py check
integration-rebuild: build
	python3 scripts/rebuild_integration.py
