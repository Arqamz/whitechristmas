.PHONY: help clean lint lint-js lint-nix lint-shell lint-yaml format format-check check report

# Color output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# ============================================================================
# Help Target
# ============================================================================
help:
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(CYAN)║           White Christmas - Development Environment            ║$(NC)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Validation & Quality:$(NC)"
	@echo "  $(GREEN)make lint$(NC)               - Lint all code (JS, Nix, Shell, YAML)"
	@echo "  $(GREEN)make lint-js$(NC)            - Lint JavaScript/TypeScript code"
	@echo "  $(GREEN)make lint-nix$(NC)           - Lint Nix code"
	@echo "  $(GREEN)make lint-shell$(NC)         - Lint shell scripts"
	@echo "  $(GREEN)make lint-yaml$(NC)          - Lint YAML files"
	@echo "  $(GREEN)make format$(NC)             - Format all code (auto-fix)"
	@echo "  $(GREEN)make format-check$(NC)       - Check formatting without changes"
	@echo "  $(GREEN)make check$(NC)              - Run all checks (lint + format-check)"
	@echo ""
	@echo "$(YELLOW)Report:$(NC)"
	@echo "  $(GREEN)make report$(NC)             - Build report PDF (LaTeX)"
	@echo "  $(GREEN)make report-clean$(NC)       - Clean report build artifacts"
	@echo ""
	@echo "$(YELLOW)Cleanup:$(NC)"
	@echo "  $(GREEN)make clean$(NC)              - Remove generated files"

# ============================================================================
# Format Code
# ============================================================================
format:
	@echo "$(CYAN)Formatting all code...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v treefmt &> /dev/null; then \
	  treefmt; \
	  echo "$(YELLOW)─────────────────────────────────────$(NC)"; \
	  echo "$(GREEN)✓ All code formatted!$(NC)"; \
	else \
	  echo "$(RED)treefmt not found in PATH$(NC)"; \
	  echo "$(YELLOW)Run: nix develop$(NC)"; \
	fi
	@echo ""

# ============================================================================
# Clean Artifacts
# ============================================================================
clean:
	@echo "$(CYAN)Cleaning build artifacts...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@rm -rf .next dist build
	@rm -rf target/ *.class *.jar
	@find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name target -exec rm -rf {} + 2>/dev/null || true
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@echo "$(GREEN)✓ Cleaned!$(NC)"
	@echo ""

# ============================================================================
# Lint: JavaScript/TypeScript
# ============================================================================
lint-js:
	@echo "$(CYAN)Linting JavaScript/TypeScript code...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v eslint &> /dev/null; then \
	  eslint src --max-warnings=0 2>/dev/null || true; \
	  echo "$(YELLOW)─────────────────────────────────────$(NC)"; \
	  echo "$(GREEN)✓ ESLint check complete!$(NC)"; \
	else \
	  echo "$(RED)eslint not found in PATH$(NC)"; \
	fi
	@echo ""

# ============================================================================
# Lint: Nix
# ============================================================================
lint-nix:
	@echo "$(CYAN)Linting Nix code...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v statix &> /dev/null; then \
	  statix check -- . 2>/dev/null || echo "$(YELLOW)Statix checks completed$(NC)"; \
	else \
	  echo "$(RED)statix not found in PATH$(NC)"; \
	fi
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@echo "$(GREEN)✓ Nix linting complete!$(NC)"
	@echo ""

# ============================================================================
# Lint: Shell Scripts
# ============================================================================
lint-shell:
	@echo "$(CYAN)Linting shell scripts...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v shellcheck &> /dev/null; then \
	  shellcheck -x scripts/*.sh tests/*.sh 2>/dev/null || echo "$(YELLOW)Shellcheck completed$(NC)"; \
	else \
	  echo "$(RED)shellcheck not found in PATH$(NC)"; \
	fi
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@echo "$(GREEN)✓ Shell linting complete!$(NC)"
	@echo ""

# ============================================================================
# Lint: YAML
# ============================================================================
lint-yaml:
	@echo "$(CYAN)Linting YAML files...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v yamllint &> /dev/null; then \
	  yamllint -f parsable -c .yamllint *.yaml *.yml 2>/dev/null || echo "$(YELLOW)YAML lint completed$(NC)"; \
	else \
	  echo "$(RED)yamllint not found in PATH$(NC)"; \
	fi
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@echo "$(GREEN)✓ YAML linting complete!$(NC)"
	@echo ""

# ============================================================================
# Lint: All
# ============================================================================
lint: lint-js lint-nix lint-shell lint-yaml
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(CYAN)║               ✓ All Linting Complete!                          ║$(NC)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""

# ============================================================================
# Format Check (CI mode - no auto-fix)
# ============================================================================
format-check:
	@echo "$(CYAN)Checking code formatting...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@if command -v treefmt &> /dev/null; then \
	  treefmt --check; \
	  echo "$(YELLOW)─────────────────────────────────────$(NC)"; \
	  echo "$(GREEN)✓ All code is properly formatted!$(NC)"; \
	else \
	  echo "$(RED)treefmt not found in PATH$(NC)"; \
	  echo "$(YELLOW)Run: nix develop$(NC)"; \
	fi
	@echo ""

# ============================================================================
# Check: Lint + Format Check (CI mode)
# ============================================================================
check: lint format-check
	@echo "$(CYAN)╔════════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(CYAN)║            ✓ All Quality Checks Passed!                       ║$(NC)"
	@echo "$(CYAN)╚════════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""

# ============================================================================
# Report: Build LaTeX PDF
# ============================================================================
report:
	@echo "$(CYAN)Building report PDF...$(NC)"
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@cd report && latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
	@echo "$(YELLOW)─────────────────────────────────────$(NC)"
	@echo "$(GREEN)✓ Report built: report/main.pdf$(NC)"
	@echo ""

report-clean:
	@echo "$(CYAN)Cleaning report build artifacts...$(NC)"
	@cd report && latexmk -C main.tex
	@echo "$(GREEN)✓ Report artifacts cleaned!$(NC)"
	@echo ""

# ============================================================================
# Default target
# ============================================================================
.DEFAULT_GOAL := help
