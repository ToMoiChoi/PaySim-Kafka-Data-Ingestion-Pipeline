# ============================================================
# Makefile - Binance Crypto Streaming Pipeline
# ============================================================
# Requirements: Python 3.10+, Docker Desktop running
# First run: make install
#
# Recommended workflow:
#   make install -> make start-kafka -> make setup-pg -> make seed-pg
#   -> make run-live (terminal 1) -> make run-spark (terminal 2)
#   -> make check-db (verify)
# ============================================================

.PHONY: help install setup-bq seed-pg start-kafka stop-kafka \
        run-live run-spark run-spark-docker reconcile clean logs

# -- Colors --------------------------------------------------------
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# -- Default target ------------------------------------------------
help:
	@echo ""
	@echo "$(CYAN)+======================================================+$(RESET)"
	@echo "$(CYAN)|    Binance Crypto Streaming Pipeline                  |$(RESET)"
	@echo "$(CYAN)+======================================================+$(RESET)"
	@echo ""
	@echo "$(YELLOW)Setup$(RESET)"
	@echo "  make install        Install Python dependencies"
	@echo "  make setup-bq       Create BigQuery dataset and tables"
	@echo "  make setup-pg       Create PostgreSQL Star Schema"
	@echo "  make seed-pg        Seed dimension tables to PostgreSQL"
	@echo ""
	@echo "$(YELLOW)Infrastructure$(RESET)"
	@echo "  make start-kafka    docker-compose up (Kafka + Zookeeper)"
	@echo "  make stop-kafka     docker-compose down"
	@echo "  make logs           Tail Kafka logs"
	@echo ""
	@echo "$(YELLOW)Pipeline$(RESET)"
	@echo "  make run-live       Run live producer (Binance WebSocket)"
	@echo "  make run-spark      Run Spark locally (dual sink)"
	@echo "  make run-spark-docker  Run Spark in Docker container"
	@echo ""
	@echo "$(YELLOW)Verification$(RESET)"
	@echo "  make reconcile      Compare Kafka msgs vs BigQuery rows"
	@echo "  make check-db       Query data from Postgres DW"
	@echo ""
	@echo "$(YELLOW)Sync$(RESET)"
	@echo "  make upload-bq      Sync Postgres data to BigQuery"
	@echo ""
	@echo "$(YELLOW)Cleanup$(RESET)"
	@echo "  make clean          Remove __pycache__ and checkpoint dirs"
	@echo ""

# -- Install dependencies ------------------------------------------
install:
	@echo "$(GREEN)Installing Python dependencies...$(RESET)"
	pip install -r requirements.txt

# -- BigQuery schema setup -----------------------------------------
setup-bq:
	@echo "$(GREEN)Creating BigQuery dataset and tables...$(RESET)"
	python -m warehouse.bigquery_schema

# -- PostgreSQL schema setup ----------------------------------------
setup-pg:
	@echo "$(GREEN)Creating PostgreSQL Star Schema...$(RESET)"
	python -m warehouse.postgres_schema

# -- Seed PostgreSQL dimension tables --------------------------------
seed-pg:
	@echo "$(GREEN)Seeding PostgreSQL dimension tables...$(RESET)"
	python -m warehouse.seed_dimensions_pg

# -- Kafka infra -----------------------------------------------------
start-kafka:
	@echo "$(GREEN)Starting Kafka + Zookeeper...$(RESET)"
	docker-compose up -d zookeeper kafka
	@echo "$(YELLOW)Waiting 10s for Kafka to be ready...$(RESET)"
	sleep 10
	@echo "$(GREEN)Kafka is up.$(RESET)"

stop-kafka:
	@echo "$(YELLOW)Stopping all containers...$(RESET)"
	docker-compose down

logs:
	docker-compose logs -f kafka

# -- Live Producer (Binance WebSocket) --------------------------------
run-live:
	@echo "$(GREEN)Starting Live producer (Binance WebSocket)...$(RESET)"
	python -m producer.live_producer

# -- Spark (local mode) -----------------------------------------------
run-spark:
	@echo "$(GREEN)Starting Spark Processor (local)...$(RESET)"
	python processor/spark_processor.py

# -- Spark (Docker) ----------------------------------------------------
run-spark-docker:
	@echo "$(GREEN)Building and running Spark in Docker...$(RESET)"
	docker-compose up --build spark-processor

# -- Reconciliation ---------------------------------------------------
reconcile:
	@echo "$(GREEN)Running reconciliation check...$(RESET)"
	python -m warehouse.bq_reconcile

# -- Check Postgres Data -----------------------------------------------
check-db:
	@echo "$(GREEN)Querying Postgres Data Warehouse...$(RESET)"
	python scripts/check_pg_data.py

# -- Upload to BigQuery ------------------------------------------------
upload-bq:
	@echo "$(GREEN)Syncing Postgres data to BigQuery...$(RESET)"
	python scripts/pg_to_bq_sync.py

# -- Cleanup -----------------------------------------------------------
clean:
	@echo "$(YELLOW)Cleaning up...$(RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/spark_checkpoint_dual_sink 2>/dev/null || true
	rm -rf /tmp/bq_backup 2>/dev/null || true
	@echo "$(GREEN)Done.$(RESET)"
