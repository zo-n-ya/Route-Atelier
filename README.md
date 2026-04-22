# Route Atelier — Multi-Warehouse Dispatch Analytics Tool

## Overview

Route Atelier is an internal logistics analytics tool designed to aggregate, analyze, and visualize delivery performance data across multiple warehouses.

It replaces manual spreadsheet-based workflows by providing a centralized system for querying dispatch data, filtering batches, and generating KPI summaries.

---

## Demo

Example workflow: selecting warehouses, running analysis, and viewing KPI results.

![demo](./screenshots/demo.gif)

---

## Key Features

### Multi-Warehouse Analysis
- Supports multiple warehouses (ATL, BHM, BFM, SAV, CHS, etc.)
- Aggregates data across selected regions
- Provides warehouse-level summaries

---

### Flexible Batch Filtering

Supports multiple business modes:

- **Delivery only · 203 only**
  - Filters delivery batches based on naming rules and date window

- **All valid batches · 203 only**
  - Includes all valid operational batches excluding pickup/trucking

- **All valid batches · total**
  - Aggregates total parcel counts across valid batches

---

### Real-Time Data Integration
- Integrates with external dispatch APIs
- Fetches:
  - dispatch history
  - real-time delivery statistics
- Uses concurrent requests for faster performance

---

### KPI & Aggregation

Generates structured analytics:

- Main warehouse summary
- DSP (Delivery Service Provider) breakdown
- Driver-level aggregation
- Top driver ranking

Key metrics:
- total packages
- active drivers
- active DSPs
- top-performing warehouse

---

### Data Visualization
- Interactive charts using Chart.js
- Warehouse distribution chart
- Top driver ranking chart

---

### Excel Export
- One-click export of summarized data
- Auto-formatted Excel output
- Ready for reporting and further analysis

---

### Performance Optimization
- In-memory query caching (TTL-based)
- Concurrent API fetching (ThreadPoolExecutor)
- Efficient batch aggregation

---

## System Architecture

- **Backend:** Flask
- **Data Processing:** Pandas
- **API Integration:** External dispatch API
- **Concurrency:** ThreadPoolExecutor
- **Visualization:** Chart.js
- **Export:** OpenPyXL

---

## Project Structure
