# ============================================================
# Makefile – PaySim Kafka → BigQuery Pipeline
# ============================================================
# Yêu cầu: Python 3.10+, Docker Desktop đang chạy
# Chạy lần đầu: make install
#
# Workflow thứ tự đúng:
#   make install → make start-kafka → make setup-bq → make seed-bq
#   → make run-producer (terminal 1) → make run-spark (terminal 2)
#   → make reconcile (kiểm tra)
# ============================================================

.PHONY: help install setup-bq seed-bq start-kafka stop-kafka \
        run-producer run-spark run-spark-docker reconcile clean logs

# ── Colors ──────────────────────────────────────────────────
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# ── Default target ──────────────────────────────────────────
help:
	@echo ""
	@echo "$(CYAN)╔══════════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║       PaySim Kafka → BigQuery  |  Sprint 2           ║$(RESET)"
	@echo "$(CYAN)╚══════════════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@echo "$(YELLOW)🔧  Setup$(RESET)"
	@echo "  make install        Install Python dependencies"
	@echo "  make setup-bq       Create BigQuery dataset & tables"
	@echo "  make seed-bq        Seed dimension tables to BigQuery"
	@echo "  make setup-pg       Create PostgreSQL Star Schema"
	@echo "  make seed-pg        Seed dimension tables to PostgreSQL"
	@echo ""
	@echo "$(YELLOW)🐳  Infrastructure$(RESET)"
	@echo "  make start-kafka    docker-compose up (Kafka + Zookeeper)"
	@echo "  make stop-kafka     docker-compose down"
	@echo "  make logs           Tail Kafka logs"
	@echo ""
	@echo "$(YELLOW)🚀  Pipeline$(RESET)"
	@echo "  make run-producer   Run Kafka producer (publish PaySim events)"
	@echo "  make run-spark      Run Spark locally (BigQuery sink)"
	@echo "  make run-spark-docker  Run Spark in Docker container"
	@echo ""
	@echo "$(YELLOW)📊  Verification$(RESET)"
	@echo "  make reconcile      Compare Kafka msgs vs BigQuery rows"
	@echo ""
	@echo "$(YELLOW)🧹  Cleanup$(RESET)"
	@echo "  make clean          Remove __pycache__ & checkpoint dirs"
	@echo ""

# ── Install dependencies ─────────────────────────────────────
install:
	@echo "$(GREEN)📦 Installing Python dependencies...$(RESET)"
	pip install -r requirements.txt

# ── BigQuery schema setup ─────────────────────────────────────
setup-bq:
	@echo "$(GREEN)🏗️  Creating BigQuery dataset & tables...$(RESET)"
	python -m warehouse.bigquery_schema

# ── PostgreSQL schema setup ───────────────────────────────────
setup-pg:
	@echo "$(GREEN)🏗️  Creating PostgreSQL Star Schema...$(RESET)"
	python -m warehouse.postgres_schema

# ── Seed BigQuery dimension tables ────────────────────────────
seed-bq:
	@echo "$(GREEN)🌱 Seeding BigQuery dimension tables...$(RESET)"
	python -m warehouse.seed_dimensions

# ── Seed PostgreSQL dimension tables ─────────────────────────
seed-pg:
	@echo "$(GREEN)🌱 Seeding PostgreSQL dimension tables...$(RESET)"
	python -m warehouse.seed_dimensions_pg

# ── Kafka infra ───────────────────────────────────────────────
start-kafka:
	@echo "$(GREEN)🐳 Starting Kafka + Zookeeper...$(RESET)"
	docker-compose up -d zookeeper kafka
	@echo "$(YELLOW)⏳ Waiting 10s for Kafka to be ready...$(RESET)"
	sleep 10
	@echo "$(GREEN)✅ Kafka is up!$(RESET)"

stop-kafka:
	@echo "$(YELLOW)🛑 Stopping all containers...$(RESET)"
	docker-compose down

logs:
	docker-compose logs -f kafka

# ── Producer ─────────────────────────────────────────────────
run-producer:
	@echo "$(GREEN)📤 Starting Kafka producer...$(RESET)"
	python -m producer.kafka_producer

# ── Spark (local mode) ────────────────────────────────────────
run-spark:
	@echo "$(GREEN)⚡ Starting Spark Processor (local)...$(RESET)"
	python processor/spark_processor.py

# ── Spark (Docker) ────────────────────────────────────────────
run-spark-docker:
	@echo "$(GREEN)🐳 Building & running Spark in Docker...$(RESET)"
	docker-compose up --build spark-processor

# ── Reconciliation ────────────────────────────────────────────
reconcile:
	@echo "$(GREEN)📊 Running reconciliation check...$(RESET)"
	python -m warehouse.bq_reconcile

# ── Cleanup ───────────────────────────────────────────────────
clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/spark_checkpoint_bq_v2 2>/dev/null || true
	rm -rf /tmp/spark_checkpoint_bq_native 2>/dev/null || true
	@echo "$(GREEN)✅ Done.$(RESET)"
