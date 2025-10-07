# 🧩 Analytics Dashboard Rule Specification — Tile-Level Analysis

## Overview

When a user drills down into a **specific tile** (e.g., payer-slug view such as *United Healthcare Georgia*),  
the system must evaluate and visualize negotiated rate distributions **per unique contract context**,  
since Transparency in Coverage (TiC) data produces multiple rates for the same billing scenario due to **network differences** (e.g., HMO vs PPO).

This logic defines how to group, aggregate, and visualize rate patterns for analytics dashboards.

---

## 1️⃣ Define the Core Unique Key

Each **unique rate context** should be defined by this composite key:

