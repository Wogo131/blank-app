# Chrome Web Store opportunity mining

Pipeline complet de mining d'opportunités via l'API [Chrome-Stats](https://chrome-stats.com/docs/api).

## Prérequis

- Python 3.9+ avec `requests`
- Une clé API Chrome-Stats (compte premium) dans la variable d'environnement `CHROME_STATS_API_KEY`
- Accès réseau sortant vers `chrome-stats.com` (dans Claude Code web : politique réseau
  de l'environnement → autoriser le domaine, ou mode « tous domaines »)

## Exécution

```bash
CHROME_STATS_API_KEY=<votre-clé> python3 mining/mine.py
```

## Ce que fait le script

1. **Ripe for disruption** — `POST /api/chrome/advanced-search`, 8 pages :
   `userCount` 10 k–3 M, note 2.3–4.2, ≥ 30 avis, dernière maj > 18 mois,
   extensions Google exclues (filtre client).
2. **Rising stars** — 8 pages : 1 k–80 k users, note ≥ 4.4, ≥ 10 avis,
   créées dans les 12 derniers mois.
3. **Scoring** de chaque candidat ripe :
   `log10(users) × (4.6 − note) × log10(ratingCount) × facteur d'abandon`
   (facteur = 1 à 18 mois de staleness, +1 par année supplémentaire, plafonné à 3).
4. **Analyse des reviews** (`GET /api/reviews`) pour le top 20 ripe :
   comptage de 8 familles de plaintes (cassé, perf/RAM, pubs, paywall,
   login forcé, sync manquante, feature requests, dev disparu) + extraction
   de 2-3 citations de reviews 1-2 étoiles.
5. **Sorties** dans `mining_output/` :
   - `report.md` — top 25 ripe (scores, stats, liens, plaintes, citations) + top 25 rising stars
   - `ripe_candidates.csv` et `rising_stars.csv` — listes complètes
   - `raw/` — dumps JSON bruts (recherches + analyses de reviews)

## Garde-fous

- Budget dur de 290 requêtes API (la cible < 300) avec compteur global.
- Gestion des 429 : respect de `Retry-After`, sinon backoff exponentiel (2 s → 60 s).
- Récupération sur erreurs de schéma : si une colonne de condition est refusée,
  le script sonde des variantes connues (`lastUpdate`/`lastUpdated`/`updatedAt`…,
  dates en ISO / epoch ms / epoch s) ; si aucune ne passe, la condition est
  abandonnée côté serveur et appliquée côté client sur les résultats.
