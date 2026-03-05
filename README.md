# Real Estate Listings — Egypt Price Prediction

A dataset of **~39,700 Egyptian real estate listings** (Buy & Rent) scraped from PropertyFinder Egypt in March 2026, paired with a machine learning notebook that predicts **price per sqm**.

---

##  Dataset

Scraped using Playwright. Each listing includes:

- Location (city, town, district, lat/lon)
- Property specs (type, area, bedrooms, bathrooms)
- Amenities (pool, gym, parking, etc.)
- Listing metadata (furnished, completion status, verification, images)
- Description text

---

##  Model

The notebook `property_price_prediction.ipynb` walks through:

1. **EDA** — price distributions, city breakdowns, geographic map
2. **Feature Engineering** — amenities, target encoding, date features
3. **RAG Feature** — sentence embeddings (MiniLM) + FAISS nearest-neighbor to retrieve similar listings' prices as a feature
4. **6 Models** — Ridge → Random Forest → XGBoost → LightGBM → CatBoost → Stacking Ensemble
5. **SHAP Analysis** — feature importance and interpretability

---

##  Quick Start

```bash
pip install sentence-transformers faiss-cpu xgboost lightgbm catboost shap plotly
jupyter notebook property_price_prediction.ipynb
```

Place `propertyfinder_egypt.csv` in the same directory (or `/kaggle/input/propertyfinder-egypt/` on Kaggle).
