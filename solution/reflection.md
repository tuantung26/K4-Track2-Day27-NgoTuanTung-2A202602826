# Reflection

**Which fault types were hardest to catch, and why?**

Missing lineage edges were the hardest: a graph can still contain upstream
nodes while silently omitting one expected edge, so checking only for an empty
upstream list is insufficient. I compare each observed topology with repeated
topologies for the same pipeline/transform identity and flag strict subsets.
Subtle feature drift is also difficult because it exposes one normalized shift
measurement, which can sit close to ordinary variation.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

I call the relevant metered check once per event (180 credits on the 120-event
practice stream, within its 220-credit budget). Published limits remain the
single-signal guardrails; I use corroborating marginal signals for batch and
embedding metrics, and a slightly earlier freshness threshold, rather than
lowering every threshold and increasing false alarms. Practice caught all 33
faults with no false alarms, but its easy mix does not establish private-phase
performance; with another pass I would tune robust per-entity baselines against
the unseen stream without sacrificing clean-event precision.
