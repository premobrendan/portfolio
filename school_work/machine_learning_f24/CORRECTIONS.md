# machine_learning.ipynb — correction worklist

Produced 2026-09-02 from a cell-by-cell review of the whole document. Part I was reviewed and
repaired interactively; Parts II–V were each reviewed by three independent reviewers (one per
top-level section) who re-ran the code where the data allowed and verified derivations by Monte
Carlo, symbolic algebra, or finite differences.

**Cell numbers are as of this document's creation (217 cells).** Inserting or deleting cells shifts
them — re-check before a bulk edit.

## Severity legend

| Tag | Meaning |
|---|---|
| **C** | Critical — the reported answer is wrong, or the section measures nothing |
| **M** | Major — real defect that changes numbers or breaks a clean re-run |
| **m** | Minor — latent, fragile, or cosmetic-but-real |
| **P** | Prose — explanation to rewrite (no code change) |
| **T** | Typo |

## Status by section

| Part | Section | Cells | C | M | m | P | State |
|---|---|---|--:|--:|--:|--:|---|
| I | Prostate regression | 1–12 | – | – | – | – | **fixed & verified** |
| I | Ridge on MNIST | 13–37 | – | – | – | 1 | code fixed; cell 33 prose open |
| I | LASSO on crime | 38–54 | – | – | – | 2 | code fixed; cells 46, 54 open |
| II | Bias-variance | 56–64 | – | 2 | – | 3 | not started |
| II | Kernelized regression | 65–72 | 1 | 5 | 2 | 4 | not started |
| II | Random Fourier features | 73–88 | 1 | 6 | 1 | 3 | not started |
| III | Soft margin SVM | 90–103 | 1 | 4 | 1 | 4 | not started |
| III | Naive Bayes | 104–108 | 1 | 4 | 1 | – | not started |
| III | Decision trees | 109–119 | 1 | 1 | – | 2 | not started |
| IV | K-means | 121–134 | 2 | 3 | – | 3 | not started |
| IV | K-medians (NYC) | 135–151 | 2 | 4 | 2 | 2 | not started |
| IV | Gaussian mixtures | 152–170 | 1 | 4 | 1 | 4 | not started |
| V | PCA | 172–189 | – | 4 | 1 | 4 | not started |
| V | Softmax on LFW | 190–201 | – | 4 | 1 | 4 | not started |
| V | Deep neural networks | 202–216 | – | 2 | 3 | 5 | not started |

---

# The ten that matter most

Ordered by how much they change the document. Everything here is verified, not suspected.

1. **Kernelized regression fits pure noise.** Cell 67 `f_star` uses `np.dot` on two 1-D arrays,
   which returns a **scalar** inner product, so `y = f_star(x) + error` is a constant plus noise
   with no dependence on `x`. Visible in the stored figure: all 30 points sit in y ∈ [-11, -6.5]
   with no structure. Every hyperparameter, error and plot in cells 69–72 is meaningless.
   Fix: `4*np.sin(np.pi*x)*np.cos(6*np.pi*x**2)`.

2. **The RFF verification measures nothing.** Substituting *literally zero* for the estimate gives
   the same reported "error" (2.07e-7 vs 1.74e-7). Cell 86 drops the √(2/p) scaling, normalizes by
   `np.sum(Xtr_trans**2)` (a scalar ≈ n·p/2) instead of `p`, and averages over data pairs rather
   than over random features. With all of it fixed the honest error is rms ≈ 0.018 — the expected
   O(1/√D) rate, i.e. a **passing** verification.

3. **The SVM bug you said you couldn't find** (cell 103: "uncertain where I specifically went
   wrong"). Cell 95's `DelF_w`/`DelF_b` branch on `type(y) == np.ndarray`; at `batch_size == 1`
   the scalar branch returns `-y*x[j]` and `-y`, omitting **both** the `C = 100` factor and the
   `w_j` regularizer. Finite differences: analytic 0.732267 vs numeric 72.966450, ratio 99.6.
   Effective learning rate is 100× too small with no weight decay — which is exactly why the SGD
   curve is a straight ramp and why it needed 3170 iterations, outside the professor's 500–3000
   sanity range. Patched on synthetic data: same budget, **57× lower objective**.

4. **K-means `error()` returns the cluster index.** Cell 133 does `error += min_k` where `min_k` is
   the argmin *index*; `min_val`, the actual squared distance, is computed and discarded. The
   plotted values 0.37/1.45/3.55/7.6/15.35 are just the mean of {0..K−1}. Correct values are ~1e6
   and **decrease** with K. Fix: `error += min_val`.

5. **The GMM answer changes from K=6 to K=3.** Cell 162 uses `d_K = K`; a full-covariance 2-D GMM
   has `(K−1) + K·d + K·d(d+1)/2 = 6K − 1` parameters. Under-penalizing ~6× makes BIC monotone
   increasing, so it always names the largest K tried. Corrected, BIC peaks at **K = 3** — matching
   sklearn and the data (the CSV's dropped first column is a 3-class label, 131/151/218).

6. **The NYC risk function is wrong three ways.** Cell 148 computes `risk = 10*cost_per_parking`,
   a **constant 100 for every K** (`K` is computed on the line above and never used); the distance
   coefficient is applied twice (`3*300` where 300 already is 3·100), making it 3× too large; and
   cell 151 scores the *test* set using the *training* set's `closest` array, so test point *j* is
   charged the distance to training point *j*'s lot. That last one is why the risk curve rises with
   K, which is impossible. Correct risk at K=5 is ≈439 k$/month, not the printed 1266.83.

7. **Two decision-tree answers don't render at all.** Cells 110 and 112 are **code** cells
   containing a markdown image line; IPython treats the leading `!` as a shell escape, and the
   stored output is a bash syntax error. Those two cells are the entire answers to "Drawing the
   Decision Boundaries" and the redundancy question. Change both to markdown.

8. **Naive Bayes doesn't compute in log space and mispredicts on real text.** Cell 105 writes
   `np.log((numer/denom)**x_i[d])` — it exponentiates *before* the log, so it underflows to 0 and
   `log(0) = -inf`. On a 3000-word vocabulary with ordinary counts it returns `[-inf, -inf]` and
   `argmax` silently picks class 0, where sklearn picks class 1. Fix: `x_i[d]*np.log(numer/denom)`.
   Separately, `predictProbs` returns unnormalized joint log-likelihoods (rows sum to ≈ −12000),
   not the softmax-normalized posteriors the prompt specifies.

9. **The PCA optimality proof is invalid.** Cell 176 claims the *i*th diagonal of `YᵀDY` is
   `λᵢ‖yᵢ‖²`; it is actually `Σⱼ λⱼ Y²ⱼᵢ`. The only premise used (‖yᵢ‖ = 1) holds for *any*
   orthonormal P, so the argument would prove the result for every P — false by counterexample
   (random orthonormal Q gives trace 5.159 vs the claimed 17.323). The missing step: `P* = U[:, :k]`
   makes `Y = UᵀU[:, :k] = [I_k; 0]`, so `yᵢ = eᵢ` and *only then* does it collapse to λᵢ.

10. **Bias-variance drops a factor of n.** Cell 57 defines variance and bias² as squared norms over
    θ ∈ ℝⁿ, so both are sums over coordinates. Cells 60/62 give per-coordinate answers: `V[X] = σ²`
    should be `nσ²`, and `bias² = θ²/100` should be `‖θ‖²/100`. Monte Carlo at n=7: true `V[X]` is
    20.22, not σ² = 2.89.

---

# Part I — remaining prose (code already fixed and verified)

**Cell 33 — random cosine features (P).** The diagnosis ("randomness of the transform being less
uniform for the data our model isn't trained for") described the seeding bug, now fixed; validation
at p̂ is 4.4%. The conclusion inverts — random features **do** improve, 14.66% → 4.49% test error.
"7 different p-values" should be 6. The runtime note is stale: the 6-point sweep now runs in 11 s,
and the p=6000 observation was the bug, not a finding.

**Cell 46 — LASSO efficiency (P).** The algebra is correct; keep it. `X_jᵀX_{-j}` is a length-94
vector, not "two scalars", and rearranging alone stays O(N·d) — the saving comes from hoisting the
Gram matrix out of the loop. Stale numbers: it cites 1000 runs at λ=1; the notebook does 10 at λ=5.
New measured: 9.35 s → 0.34 s, 27.8×, identical iteration counts.

**Cell 54 — λ=30 discussion (P).** Premise gone: "different every time" was the stopping bug; the
converged solution is unique. "A lower λ would be more suitable" is unsupported — test MSE bottoms
out near λ ≈ 10–15 and worsens below it. New answer: most positive `PctIlleg` +0.06976, most
negative `PctKids2Par` −0.06744, 9 nonzeros. The question asks you to *discuss*: these are
mirror-image family-structure proxies of near-equal magnitude and opposite sign, which is the
correlation-vs-causation point cell 38 raises.

**Open question (not an error).** No intercept: the docstring claimed "first column is ones" but
`X[:,0]` is `population`; features are centered, y has mean 0.2376. Confirm that's intended.

---

# Part II — Model Complexity and Kernel Methods

## Bias-Variance Trade-off (56–64)

- **M** Cells 60, 62 — missing factor of n / the norm. See item 10 above. Carry the sum over i
  through both cells, since the comparison in 59–60 is over the full vector.
- **T** Cell 60 line 2 — `(E[X] − θ)² = (θ − θ)`, the square is dropped (result 0 still correct).
- **P** Cell 64 never answers "which is better" and never uses the given bound. Your own 56/58 give
  it in one line: shrinkage − natural = `−(19/100)nσ² + (1/100)Σθᵢ²`, negative iff
  `Σθᵢ² < 19nσ²`; the hypothesis gives `Σθᵢ² < 10nσ² < 19nσ²`, so shrinkage wins with room to
  spare (break-even is at `σ² > θᵢ²/19`, weaker than assumed).
- **P** Cell 64 — "happy medium" is vague; for `θ̂ = cX` the optimum is exactly
  `c* = Σθᵢ²/(Σθᵢ² + nσ²)`.
- **P** Cell 64 — the opening claim that variance changes more than bias is asserted, not shown.
- **T** Cell 64 "occuring" → "occurring", "v.s." → "vs."; cells 60/62 use plain `\theta` where the
  section's bold `\mm\theta` is meant, which is what hides the missing sum.

**Verified correct:** cell 58's decomposition, line by line and numerically. Cell 60's `bias²(X)=0`,
cell 62's per-coordinate forms and the `V[cX] = c²V[X]` scaling.

## Kernelized Regression (65–72)

- **C** Cell 67 `f_star` — see item 1 above.
- **M** Cell 67 `gamma_heuristic` returns the **median** squared distance, not its inverse as the
  prompt specifies. Returns ≈0.098 where the heuristic is ≈10–15, so the grid
  `linspace(γ−1, γ+1, 10)` is centered ~150× too low and **five of ten values are negative** — and
  a negative γ gives `exp(+γ‖x−z‖²)`, not a PSD kernel. On correct data the student's grid yields
  LOOCV 5.700 vs 1.966 for a grid around `1/median`: no better than predicting the mean.
- **M** Cell 67 `pred_rbf` calls **`kernel_poly`**, using γ as a polynomial degree. So the RBF path
  fits α with the RBF matrix and predicts with `(1+x·z)^γ`. Verified: 5.2296 vs the correct 4.8530.
- **M** Cell 67 `loo_mse` divides by a **global `n` from another problem** (`return sum/n`; `n` is
  never a parameter). Top-to-bottom that is cell 18's `n = 60000` from MNIST, so every reported
  LOOCV error is 2000× too small. The argmin is unaffected; the printed numbers are not.
- **M** Cell 72 — `NameError`: assigns `best_lamb_rbf`, uses `best_lam_rbf` (which exists nowhere).
- **M** Cell 72 plots — `ax1.plot(f_poly)` with no x plots against **index 0…29** while the scatter
  sits at x ∈ [0,1]; `x` is never sorted so points connect in random order; `f_poly`/`f_rbf` are the
  leave-one-out predictions rather than f̂ fit on all n and evaluated on a fine grid; and the true
  `f_*(x)` is **never plotted**, though cell 71 requires it.
- **m** Cell 67 degree grid `linspace(5,25,10)` gives non-integer degrees (hence the reported
  11.666666666666668). Use `np.arange(5,26)`.
- **m** `np.infty` was removed in NumPy 2.0 — one more reason the section won't re-run today.
- **P** Cell 70 — the reported d and λ ranges come from noise; `[10,12]` contains exactly one grid
  value. The claim that the γ heuristic "was used" is inaccurate (the median, not its inverse, was).
- **P** Cell 69's printed "min error" values are not MSEs.
- **P** Cell 72's hand-typed hyperparameters don't match what the search returned.
- **Note:** the stored outputs **cannot be reproduced from the current source** — they require
  `x` of shape (n,1) and a `gamma_heuristic` without the trailing `[0]`, yet the figure proves the
  `f_star` bug was already present. Treat 0.909 / 1.037 / λ=0.000599 / d=11.67 / γ=0.185 as
  unreproducible rather than merely wrong.

**Verified correct:** `kernel_poly` and `kernel_rbf` match reference formulas exactly;
`kernel_regression` solves the right system (cross-checked against sklearn `KernelRidge`, dual
coefficients to 6.6e-08); **the LOOCV structure is sound** — folds genuinely disjoint, hyperparameter
scored on held-out data, `np.delete` alignment correct; `grid_search` tracks the argmin correctly.

## Random Fourier Features (73–88)

- **C** Cells 86–88 — the verification measures nothing. See item 2 above. Component bugs: missing
  √(2/p) scaling; `inner_prod` normalized by `np.sum(Xtr_trans**2)` ≈ n·p/2 instead of `p`;
  `expect = np.mean(inner_prod)` averages over data pairs, not random features, so one constant is
  compared against 20 different pairs whose rbf values span 4.4e-9 to 1.8e-3.
- **M** Cell 86 — `np.random.normal(0, .1, ...)`: numpy's second argument is the **standard
  deviation**, but the prompt specifies σ² = 0.1. Use `np.sqrt(0.1)`.
- **M** Cell 87 — `gam = .1` is inconsistent with the sampling distribution by 20×. For
  `w ~ N(0, s²I)` the induced kernel is `exp(−s²‖δ‖²/2)`, so `gam = s²/2` = 0.005 as coded
  (0.05 under the intended σ²).
- **M** Cell 81 lines 3–6 — the Bochner derivation. Line 3 writes `1/(2σ⁻²)` for `1/(2σ²)`
  (load-bearing typo); line 4 consequently has `−1/(2γ²)wᵀw` where `−(γ²/2)wᵀw` is needed, leaving
  the line internally inconsistent (constant for sd 1/γ, exponent for sd γ); line 5's completed-square
  constant should be `γ⁴δᵀδ`, not `δᵀδ`; **line 6 does not follow from line 5** — the sign is flipped
  (a positive quadratic, so the integral diverges) and the shift should be `w − iδ/γ²`, not `w − iδ`.
  Verified in 1-D: line 6 gives −3.333 − 10.861i, line 5 gives +3.333 + 3.910i.
  **Pointer:** from `−γ²wᵀw/2 + iwᵀδ`, factor out `−γ²/2` and complete as
  `−(γ²/2)‖w − iδ/γ²‖² − ‖δ‖²/(2γ²)`. That reproduces lines 7–9 exactly as written — **the final
  answer is correct**, only lines 3–6 need repair.
- **M** Cell 83 — the i.i.d. justification names the wrong randomness. `x` and `x'` are **fixed**;
  the probability is over the D i.i.d. draws of `(wᵢ, bᵢ)`. This is the central point the question
  asks for.
- **M** Cell 83 — boundedness is claimed for the wrong variable and the constant 8 is never
  obtained. Hoeffding needs the **product** `2cos(·)cos(·) ∈ [−2,2]`, range 4; then
  `2exp(−2Dε²/16) = 2exp(−Dε²/8)` falls out. The rate in D is asserted, not derived.
- **m** Cell 75 drops the imaginary term without justification (cite `k_p` real, or p(w) symmetric).
- **m** Cell 85 — stored output doesn't match the source (no `print` in it). Stale.
- **P** Cell 88 — "error of magnitude [1e-7,1e-8], making this an exceptional estimate" is
  unsupported; both quantities are ≈0. The honest number is ~1e-2 rms, which is still a good result.
- **P** Cell 88 describes a mean *squared* error as an "error of magnitude", and states a range
  from a single run.
- **T** Cell 77 misplaced conditional bar; cell 79 `E_w` should be `E_{w,b}`; cell 79 "since 3.1.2
  holds" is a stale cross-reference.

**Verified correct:** Step 1's result; **Step 2 (cell 77) is fully correct**, including the 1/(4π)
chain-rule constant (Monte Carlo 0.5923 vs 0.5931); Step 3 correct; cell 81 lines 1–2 and 7–9
correct; **the stated bound `2exp(−Dε²/8)` is the right one**, just not derived; cell 86's
broadcasting is correct; cell 87's index range has no off-by-one.

---

# Part III — Classification

## Soft Margin SVM (90–103)

- **C** Cell 95 — the scalar-branch gradient. See item 3 above.
- **M** Cell 95 — mini-batch slicing off by one: `low = batch_size*k + 1` should be `batch_size*k`.
  Simulated over a full cycle (N=6414, β=20): batch sizes are only ever 13 or 19, **never 20**, and
  321 samples — every 20th, `[0, 20, 40, …]` — are **never used**, 5.00% of the data.
- **M** Cell 95 — `k` starts at 1 and `batch_x = x[k]` is read before the increment, so SGD never
  visits index 0 within the run.
- **M** Timing is not comparable. Every iteration calls `Loss` over all 6414 samples **three times**
  (once for the log, twice inside `ConvergeCriterion`). Measured: 3 loss calls 13.2 ms vs full-batch
  gradient 605.8 ms, mini-batch 1.9 ms, SGD 0.10 ms — so the fraction of measured time that is
  actual algorithm is 98% / 13% / **0.7%**. The reported SGD and mini-batch wall times are mostly
  loss logging.
- **M** Stale state — cell 95 (the definitions) has execution count **19** while cell 96 (the batch
  run) has **5**. The blue curve and the 100.485 s figure came from a version of `Loss`/
  `GradientDescent` that no longer exists; a clean re-run won't reproduce them.
- **m** Cell 96 uses ε = 0.2, not the specified 0.25 (self-documented).
- **P** Cell 103 — "as expected, the stochastic plot has a very constant, linear downward direction"
  is **not** expected; it is the signature of the gradient bug, and it contradicts the same
  paragraph's "due to poor implimentation on my part". Correct SGD shows a sharp noisy initial drop.
- **P** Cell 103 — "this clearly seems to balance out for mini batch and stochastic" is unsupported:
  122.9 s vs 47.8 s is a 2.6× gap. The per-iteration times look equal only because ~99% and ~87% of
  them are loss logging; real gradient cost differs ~20×.
- **P** Cell 103 — iteration counts are attributed to batch size, but the three runs use ε = 0.2 /
  0.001 / 0.01 (a 200× span) and two different criteria.
- **P** Cell 103 — "where the batch plot stopped short of": batch ends at ≈253,000, **below**
  stochastic's ≈318,000.
- **T** "stochasitc", "implimentation"; cell 95 docstrings "indicies", "varients"; cell 98 "im".

**Verified correct:** batch and mini-batch gradients are exact (finite differences to 1e-7),
regularizer included once in `∂/∂w_j` and correctly absent from `∂/∂b`; `Loss` implements the stated
objective and is what's plotted, on the same data for all three; the "use t−1 values" requirement is
satisfied with no in-place contamination; `ConvergeCriterion` and the 0.5-smoothed variant are both
correct; Sanity Check 1 passes.

## Multinomial Naive Bayes (104–108)

- **C** Cell 105 — the fake log space. See item 8 above.
- **M** Cell 105 — `predictProbs` returns unnormalized joint log-likelihoods, not posteriors. The
  prompt specifies the max-subtraction softmax and rows summing to 1; there is no softmax anywhere.
  Cell 108 stores the result as `yposterior_NB` and never checks it.
- **M** Cell 105 — `log_theta` initializes `value = 1` instead of 0, inflating every class
  log-likelihood by exactly +1 (verified: student − sklearn `_joint_log_likelihood` == 1.0 exactly).
  Cancels in `argmax`, so predictions are unaffected, but the returned numbers are wrong.
- **M** Cell 105 — class indexing assumes labels are exactly `0..K−1`. `prior()` indexes by label
  **value**, `predict()` iterates by **index**. Labels `[1,1,2,2]` → `IndexError`; `[-1,-1,1,1]`
  silently predicts all 1s (accuracy 0.5 vs sklearn's 1.0) because −1 wraps.
- **M** Cell 106 — `NameError` on a clean run: `from sklearn import datasets` is commented out but
  `datasets.load_digits()` is called, and no other cell binds that name.
- **m** Cell 105 — the `alpha` argument is dead and silently mutated. `prod_theta` unconditionally
  overwrites `self.smoother`, so `NaiveBayes(alpha=5)` is reset to 1 after any non-log call, and
  `useAddOneSmoothing=True, alpha=0` applies no smoothing. The log and non-log paths can disagree.
- **T** "implimentation" (cells 107, 108 — **appears in the stored output**, so it's visible in the
  rendered portfolio); `logLikelyhood` → likelihood (public API, 6×); "feautures"; docstrings say
  the return is "an integer" when it's a float.

**Verified correct — and these are the classic error spots:** add-one smoothing arithmetic is right
in both paths, `(N_cd + α)/(N_c + α·D)`, with the denominator adding `α·D` not `α` (matches
sklearn's `feature_log_prob_` to 1.8e-15); priors estimated and used (match to 8.9e-16); counts
enter as exponents correctly with the class-independent multinomial coefficient correctly omitted;
the smoothing flag does disable smoothing; empty documents handled; the sklearn check is genuine
(accuracy 0.8909899888765295 both, prediction agreement 1.0) — it just doesn't cover `predictProbs`,
the underflow path, or non-0-indexed labels.

## Decision Trees (109–119)

- **C** Cells 110, 112 — markdown images in code cells. See item 7 above. Note Google Drive
  `uc?export=view` links often don't render inline even from markdown; prefer `thumbnail?id=…` or a
  repo-hosted image.
- **M** Cell 115 — the information-gain calculation is wrong for **A and B**: each branch sums over
  only one class label, dropping the complementary `−p log p` term. Reconstructing the counts from
  your own quoted fractions plus `H(S)=1` (which forces 3 positive / 3 negative):
  IG(A) = **0.0817**, not 0.459/0.541 (your 0.459 is exactly one branch's entropy counted once);
  IG(B) = **0**, not 0.5 — B is completely uninformative; IG(C) = 1 ✓. The conclusion (split on C)
  survives, but the question says "show your calculations".
- **m** Cell 117 — the non-empty-child proof is **directionally correct** (contrapositive, right
  direction). Two gaps: for a feature with t > 2 values, "not every child non-empty" means *some*
  child is empty, not that all examples go to one — the "WLOG f₁" line silently assumes the binary
  case; and `⊂` is used where `⊆` is meant, with `(0/k)H(∅)` needing the `0 log 0` convention.
- **P** Cell 113 — three problems. (i) "The accuracy of either tree is only a matter of
  interpretation" is wrong: same boundaries ⇒ same function ⇒ **identical** accuracy on any dataset,
  which is exactly what the question asks. (ii) It answers the wrong question — the prompt asks about
  the cost of *finding* an accurate tree, not evaluating one; redundancy means many distinct trees
  encode the same function, enlarging the search space (finding the optimum is NP-hard).
  (iii) The benefit is missed: many structural routes to the same good function make greedy search
  more forgiving of a bad first split.
- **P** Cell 119 — several false claims. (i) "we must sort through each and every choice" — no; IG
  is still computed for every pair at every node, only the `argmax` becomes a uniform draw, so
  per-node cost and the stopping rule are unchanged. (ii) "a height of at least k" is **backwards** —
  k is an *upper* bound, and your own part-3 example refutes it (drawing C at the root gives height
  1 with k=3). (iii) "number of decisions equal to `∏|Fᵢ| − 1`" is false as an equality — that's the
  fully expanded tree, but the algorithm stops at pure nodes. (iv) The actual reason is never given:
  greedy max-IG reduces label uncertainty fastest, so fewer splits reach pure leaves; a random
  positive-IG split reduces entropy more slowly, hence larger trees. (v) The accuracy answer never
  separates train from test — training accuracy is *unchanged* (still drives to pure leaves), while
  average *test* accuracy is expected somewhat lower because the larger tree overfits more.
- **T** "implimentation", "guarenteed"/"guarentee", "a ID3"; cell 115 uses a raw Unicode ≈ inside an
  `align*` block (use `\approx`).

**Verified correct:** cell 115's `H(S) = 1`, `H(S|C) = 0`, `IG(C) = 1`, the conditional-entropy
definition as written, and the final answer; cell 117's contrapositive choice, implication direction,
algebra, and closing equivalence claim.

**Unassessable:** the three professor figures and your two answer images are unreachable offline
*and* don't render. Sub-questions 1 and 2 can't be judged until the cell-type fix lands.

---

# Part IV — Unsupervised Learning

## K-Means Clustering (121–134)

- **C** Cell 133 — `error()` returns the cluster index. See item 4 above.
- **C** Cell 128 — `get_centers` has **no empty-cluster handling**, and this is the actual cause of
  cell 122's "issues generating clusters for higher K". An empty cluster gives `0/0` → a NaN row →
  `scipy.linalg.norm`'s `asarray_chkfinite` raises. Confirmed: at K=32, **seeds 0, 1 and 3 all
  produced empty clusters on the very first iteration**; only seed 2 survived. Note that with
  `np.linalg.norm` there would be no exception at all — `nan < min_dist` is always False, so `min_j`
  would silently retain the previous point's value. The scipy import is the only thing making this
  loud.
- **M** Cell 128 — `get_initial_mu` samples from U[0,1] while MNIST pixels are 0–255, the **root
  cause** of the above. Data norms are ≈2359 while centers sit in the unit cube, so all K centers
  are effectively identical. Iteration-0 distortion is 3.432e10 = **99.5% of Σ‖x‖²**, i.e. worse
  than a single center at the global mean (2.06e10). Use Forgy (K random rows) or k-means++.
- **M** Cell 134 — `.reshape((3,a))` where a **transpose** is needed. `np.array(error_list)` is
  (5,3); reshape re-reads the row-major buffer, so `plt.plot(error_list[0], error_list[1])` would
  use x = [2, 0.37, 0.4, 4, 1.45]. Fix: `.T`.
- **M** Cells 133/134 — stored outputs don't match stored source (cell 133's stream prints
  "2 4 8 16 32" but has no `print`; the cell-130 figure is provably not what its source produces).
  Both were edited after their last execution.
- **M** Cell 124 part 2 — **the proof is circular.** "By the definition of μⱼ, we have
  μⱼ = Σx/|S|" — μⱼ is *not* defined as the cluster mean; that it *equals* the mean is the
  conclusion. The whole of part 2 is an identity rewrite of the assumed answer, and the opening
  "assume J is minimal" is never used. Set `∇_{μⱼ} J = −2Σᵢ rᵢⱼ(x⁽ⁱ⁾ − μⱼ) = 0` (convex quadratic)
  or complete the square.
- **m** Cell 124 part 2 — `I` is defined by the wrong condition (`Jᵢ` minimal is an *assignment*-step
  property, but `{rᵢⱼ}` is fixed here); it should just be `{i : rᵢⱼ = 1}` for the j being optimized.
  As written each i gets its own jᵢ while the conclusion is about one μⱼ.
- **m** Cell 124 part 2 — misquotes the given assertion: cell 123 says each i has a unique **j**
  with rᵢⱼ=1, not a unique x⁽ⁱ⁾.
- **m** Cell 124 part 1 — the decoupling step, which is the entire content, is asserted without
  justification. One sentence fixes it: `J = Σᵢ Jᵢ` and the `Jᵢ` depend on **disjoint** blocks of
  `{rᵢⱼ}`, so the joint minimization separates.
- **m** Cell 124 part 1 — "Fix μ_{j'}" fixes one center where the prompt fixes the whole set, which
  makes the argmin over k ill-posed; and i vs i' are used interchangeably.
- **P** Cell 134 — you never notice the plotted error **increases** with K, which is mathematically
  impossible (a min over more centers can only decrease). That's a free sanity check that would have
  caught the bug. There is also **no prose conclusion at all** — the section ends on a plot.
- **P** Cell 122 — "some issues generating clusters for higher K" understates and misdiagnoses a
  deterministic consequence of two specific defects that fired on 3 of 4 seeds.
- **P** Cell 129 — the comment claims a tolerance ("or close to equal") that `ratio_stop_value = 1`
  doesn't provide; it fires only on exact float equality. Also `iter` increments after the break
  check, so the printed 54 is one less than the 55 assignment steps performed.
- **T** "implimentation" ×2, "v.s"; "Llyod's" → "Lloyd's"; `cluster_assignment`'s docstring documents
  a parameter `y` that doesn't exist; "impliment", "hueristical".

**Verified correct — the core algorithm is sound:** the assignment step uses squared L2 with a strict
`<` so the first minimum wins, and J accumulates the distance to the same center chosen as argmin;
the reported J really is the sum of squared distances (independent `cdist` recomputation agreed to
float round-off: 15159841193.181404 vs 15159841193.181458); `get_centers` computes the true mean when
non-empty; **the objective is monotonically non-increasing at every iteration** (max diff 0.0 over 49
iterations); final K=10 quality matches sklearn (1.516e10 vs inertia 1.506e10, within 0.7%); cell
131's reshape and subplot indexing are correct and the centers are clearly digit-like.

## K-Medians for NYC Taxi Parking (135–151)

- **C** Cell 148 — the fixed cost is a constant. See item 6 above. `K` is computed and never used.
- **C** Cell 151 — test risk uses training assignments. See item 6. Synthetic check: 20276.5 vs a
  correct 1564.1, with 54% of test points charged to the wrong lot. Only the K=1 point is meaningful.
- **M** Cell 148 — the distance coefficient is applied twice, 900/N instead of 300/N. Correct risk at
  K=5 is ≈438.9, not the printed 1266.83 (the mean-distance figure, 1.30 km, is fine).
- **M** Cell 145 — the convergence test sums **signed** differences: two centers moving +1 and −1
  total exactly 0.0 and the loop declares convergence. Use `np.abs(...).sum()` or `np.allclose`.
  Note `sum` here is `np.sum` only because `%pylab` shadowed the builtin 24 cells earlier; with the
  builtin it raises `ValueError`.
- **M** Cell 145 — `median_centroids` is called with the module-level global `X_rot` instead of the
  function's own `X`. It works only because the split is a prefix; any shuffled split silently
  computes medians of the wrong points.
- **M** Cell 145 — empty clusters give a NaN center, after which `argmin` returns 0 for every point
  and the clustering collapses.
- **m** Cell 151 — `plt.plot(risk_list)` plots against indices 0…9 while the axis is labelled K
  (actual values 1…10).
- **m** Cell 151 — searches K = 1…10, not 1…30 as the prompt requires ("code was taking forever").
- **P** Cell 150/151 — **no discussion at all.** The question asks you to find the optimal K; the
  section produces a curve and moves on. No K is chosen, no trade-off argument is made, and the
  curve's implausible *upward* slope is never remarked on.
- **P** Cell 149 — 1266.83 is presented as *the* risk with no units and no sanity check.
- **T** `cost_per_avarage_distance` → average; `mykmeans_plus`/`plot_kmeans` are still named
  "kmeans" though they implement K-medians; the comment "initialization for Kmeans++" describes a
  **farthest-point (maximin)** rule (`np.argmax(dist)`), not k-means++ (which samples ∝ D²).

**Verified correct — including the bug most likely to be here, which is absent:** the update uses
`np.median(centroid, axis=0)`, the **per-coordinate median, not the mean** (verified on a skewed
cluster: converged center [2,2], not [21.2,21.2]); the assignment genuinely uses L1 (discriminating
test: point (0,0) with centers (0,3) and (2,2) — L1 picks 0, L2 would pick 1, the code picks 0, and
the staircase cluster boundaries in the stored maps corroborate); **the rotation is correct**
(a point along (0.88,−0.47) maps to [0.9953, 0], `eᵀe = 0.9953·I`, and the stored plots go from
skewed to upright); no aliasing in `centroids_old`; no broadcasting accidents.

**Note on the prompt:** cell 142 labels `e1 = (0.88,−0.47)` "parallel to the avenues" and
`e2 = (0.47,0.88)` "parallel to the streets", but those labels are **swapped** relative to the
geographic description three lines earlier. You followed the geometry rather than the labels and got
it right.

## Gaussian Mixture Models (152–170)

- **C** Cell 162 — `d_K = K`. See item 5 above. The full table is in the reviewer notes; the
  headline is that BIC peaks at K=3 once corrected, agreeing with sklearn to the decimal.
- **M** Cell 166 — `random.choice(n_samples, 1, replace=False)` calls the **numpy** API on the
  **stdlib** `random` module. Cell 145's `import random` rebound the name that `%pylab` had bound to
  `numpy.random`, so this raises `TypeError: choice() got an unexpected keyword argument 'replace'`.
  **The GMM section cannot be re-run as-is.** Cell 145's own `initialize` correctly writes
  `np.random.choice`; only this redefinition uses the bare name.
- **M** Cell 166 — no covariance regularization, and it **does** crash: over 125 fits, one K=6 run
  died with `LinAlgError`. The stored K=6 plot shows the failure mode — a component sitting on ~3
  outliers near (0.64, 0.09). Add a small `reg_covar` ridge or restart on failure.
- **M** Cell 167 — stored outputs don't match the source: it has `run_sim = True` → 50 fits, but the
  outputs contain 5 prints and 5 plots and the BIC values are a single undivided run. No seed anywhere.
- **M** Cell 167 — averaging BIC over restarts is the wrong aggregation for the stated purpose. Cell
  163 asks for restarts to avoid local optima; the fix for that is **max**, not mean. The optima are
  far apart (K=3 ranged 521.17 to 760.89 over 25 restarts), and the farthest-point init is
  deterministic after the first pick, so the restarts explore very little.
- **m** Cell 159 — `log_likelihood` reads the **global** `Pi`, ignoring its own `Pi_opt` argument.
  Right answer in cell 167 only because that cell assigns the global immediately before the call.
- **M** Cell 153 part 2, forward direction — **the key step is unjustified.** "Suppose KL = 0. Then
  ∀z either q(z)=0 or log(q(z)/p(z))=0" does not follow: a sum being zero implies every term is zero
  only if the terms are individually non-negative, and `q log(q/p)` is negative wherever q < p.
  **Pointer:** argue equality in part 1's chain instead — `log a ≤ a−1` holds with equality **iff
  a = 1**, and part 1 shows the chain collapses to 0, forcing `p/q = 1` on the support of q. You
  never state the hint's equality condition anywhere, and that omission is the gap.
- **m** Cell 153 part 1 — `log(p/q)` is undefined when q(z)=0; restrict to `{z : q(z) > 0}` or invoke
  `0 log 0 = 0`.
- **P** Cell 170 — "got K=6 … so I choose K=6" is an artifact. With `d_K = K` the penalty grows only
  ~3.11 per unit K, so the criterion is monotone increasing and **always names the largest K tried**
  — the model selection isn't selecting anything. The stored K=6 plot itself shows the overfit.
- **P** Cell 170 — the cited numbers don't match the stored outputs ("hovering around 759", K=6 at
  "759.9"; the stored list has K=6 at 762.68).
- **P** Cell 161 — the `#### Solution` cell is **empty**. Cell 160 asks two things; the first
  ("what is d_K?") is answered only by a code comment, and answered wrongly.
- **P** Cell 155 — "θ^{t+1} = log(ℓ(θ^{t+1})) = L(θ^{t+1})" is a category error (a parameter vector
  equated with a scalar), and L is the EM Q-function, not log ℓ.
- **T** `\delta` for partial derivatives throughout 151–153; cell 155's displayed objective is missing
  the outer `Σ_k`; cell 157 switches subscripts k → c mid-derivation and drops the sample superscript;
  `0*(-2)` should be `0*2`.

**Verified correct:** KL part 1 and the reverse direction of the iff; **all three EM updates**
(π̂, μ̂, Σ̂) match the responsibility-weighted forms, the Lagrange multiplier is correctly derived
and applied, and **the responsibilities are correctly held fixed — the M-step is not circular**;
`log_likelihood` is the correct observed-data form and is numerically exact against a `logsumexp`
reference and against sklearn at every K (≈1e-10); **EM is monotonically non-decreasing at every
iteration** for K=2..6; the EM implementation's optima match sklearn exactly (K=2: 514.8293 both;
K=3: 760.8861 both); the BIC *sign* convention is consistent — only the count is wrong.

---

# Part V — Dimensionality Reduction and Neural Networks

## Principal Component Analysis (172–189)

- **M** Cell 176 — the optimality proof is invalid. See item 9 above.
- **M** Cell 189 — the "Original" column plots the **mean-centered** image. Cell 180 overwrites
  `faces = faces - faces_mean` and never restores it, while every reconstruction column adds the mean
  back — so column 1 and columns 2–7 aren't on the same scale, which is exactly the comparison the
  figure exists to make. Visible in the rendered figure (column 1 is harsher).
- **M** Cell 189 — `vmin=0, vmax=1` omitted though cell 187 explicitly says to pass them, so every
  panel autoscales independently and brightness isn't comparable across k.
- **M** Cell 182 — the **sum of all eigenvalues is never reported**. Cell 178 asks for λ₁, λ₂, λ₁₀,
  λ₃₀, λ₅₀ *and* Σλᵢ; the loop prints cumulative partial sums instead. (It's recoverable from the
  plot as ≈455.1, internally consistent from two independent points.)
- **M** Cells 177/180 — centering deviates from the Σ the problem **defines**. Cell 177 specifies the
  *uncentered* second moment `Σ = (1/n)Σxᵢxᵢᵀ`, and the question "why is λ₁ much larger than the
  others" only makes sense uncentered. Your λᵢ is effectively the intended λᵢ₊₁ (verified on a proxy
  dataset: uncentered[i+1]/centered[i] ≈ 0.996–1.000), which your own output corroborates — λ₁=167.47
  and λ₂=160.94 differ by 4%, impossible for the uncentered matrix. Centering is the statistically
  correct thing to do, so this is "answers a different question than asked", but it's the root cause
  of the two explanation errors below.
- **m** Cells 182/188 — not idempotent. 178 does `.T` then row-indexes; 184 is a bare
  `eig_vec = eig_vec.T`. Running either twice, or 178 after 184, flips orientation silently.
- **m** Cell 176 — dropped superscript (`P` should be `P*`); "each row of `A = YᵀD` is `yᵢλ⃗`" is
  ambiguous and as written wrong (row i is the elementwise product).
- **P** Cell 183 — "λ₁ represents facial structure, i.e. a gray scale that generates a generic face"
  is wrong for the computation performed. The generic face is the **mean**, which cell 180 subtracts;
  after centering the top direction is the dominant **illumination** mode. Your own cell 185 figure
  shows it: EV2 is the classic left-bright/right-dark split, EV3 top/bottom.
- **P** Cell 183 — implicitly accepts that λ₁ ≫ the rest, which your own numbers contradict
  (167.47 vs 160.94). Worth flagging that centering removed the mean-face component.
- **P** Cell 186 — attributes the eigen-directions to identity when they mainly encode lighting
  (38 people × 64 lighting conditions). You do note "the second looks like lighting" but frame it as
  incidental rather than the organizing principle. The defensible calls: EV6 (brow) and the
  nose-dominated EV3/EV10.
- **P** Cell 183 — "eigenvalues capture the variance associated with each feature": they give the
  variance along each principal **direction**, not each pixel.
- **T** "vairance", "eigen values", "matricies", "conjuntion", "eigen vector"; `$ith$` → `$i$th`;
  `\mathcal{R}` used for the reals where `\mathbb{R}` is meant (cells 174 and 176).
- **m** Cell 179 is Colab/Drive-specific (`google.colab.drive`) — dead weight locally.

**Verified correct:** **cell 174's proof is correct**, including a from-scratch proof of the cyclic
trace property rather than assuming it; Σ orientation and n are right (8064×8064 from n=2414);
`eigh` is the right routine and `np.real` is a harmless no-op; **the sorting is descending and
correctly paired — the classic "eigh returns ascending, take the first k" bug is absent**, because
`.T` is applied before the row indexing; the fractional reconstruction error formula and plot are
correct and internally consistent; `reshape((84,96)).T` is the correct inverse of column-major
flattening (verified against a non-square test case) and the rendered faces are upright; the
reconstruction `mean + UUᵀ(x − mean)` matched sklearn's inverse transform to 1e-8 for k=1,2,5,10.

## Softmax Regression on LFW (190–201)

- **M** Cell 197 — the plotted "NLL" is **not** the negative log-likelihood.
  `for p in probs: log_like -= np.log(p)` accumulates `−Σ_k log p_k` over **all K classes**, not
  `−log p_y`. Because it rewards uncertainty it *increases* as the model improves — the stored panel
  rises 33 → 55 while misclassification falls 0.66 → 0.01. Fix: use only the true-class term, which
  is exactly what `sum_error` already computes. Part (b) is therefore answered by the **third** panel.
- **M** Cell 197 — the convergence criterion is meaningless *and* evaluated on the test set.
  `abs(sum_error - prev_sum_error) < 0.1` on a sequence that oscillates 283 → 137 → 122 → 151 → 169
  → 120 → 111 is a chance crossing, not convergence. Your cell 201 ("really a grab bag") describes
  the bug rather than inherent randomness. Secondary: stopping is decided on the test set and the
  test error at that epoch is the headline number, so 13.6% is selected on the data it evaluates.
- **M** Cell 197 — `get_probs` doesn't subtract the row max. Verified to return
  `[0., nan, 0., 0.]` on scaled-up weights. It didn't fire in the stored run (W starts at zeros,
  lr 0.01, pixels in [0,1]) — but it's the one-line fix the prompt asks about.
- **M** Cell 199 — `plt.plot(data_train[0], data_train[1], data_test[1])`: the third positional
  argument is the **format-string** slot, so the test array becomes a new series with default x. The
  test curve is shifted one epoch left of train in all three panels. Fix: `plt.plot(x, tr, x, te)`.
- **m** Cell 197 — train metrics are measured with a **moving** W (averaged over 912 different weight
  vectors) while test uses the end-of-epoch W, so the train/test comparison isn't like-for-like.
- **M** Cell 195 — the derivation is correct **through** the case split, then breaks. The line "Thus
  L_k(W) is a vector and L_k(W)[i] = …" indexes a feature-space vector by `i`, which is already the
  sample index, and case-splits on `k = i`; the split belongs **inside** the sum, keyed on
  `k = y⁽ⁱ⁾`. The final boxed result is then the wrong object: shape stated **K×K** where it must be
  **K×d**; a 1 subtracted on *every* diagonal entry rather than only the true class; and
  `x⁽ⁱ⁾ ·` scalar-multiplying a K×K matrix doesn't type-check — the per-sample term is an **outer
  product** with `(p⁽ⁱ⁾ − y⃗⁽ⁱ⁾)`. The one-hot vector the prompt asks you to use never appears.
  **Your code in cell 197 already implements the correct form** (`error = probs - onehot_y`), verified
  against central finite differences to **3.0e-10** — only the write-up is wrong.
- **m** Cell 195 — `L` is used as the log-likelihood inside and the negative mean log-likelihood
  outside, so the `−1/N` is applied twice over (they cancel); and the second line of the align block
  uses a fixed class `c` for all samples where it should be `y⁽ⁱ⁾`.
- **P** Cell 201 — "we reached convergence at epoch 125" **contradicts the stored output**: cell 198
  prints "Ending epoch = 104" and cell 200 prints "at Convergence Epoch (104)".
- **P** Cell 201 — "total cross validation error" is a misnomer; there's no cross-validation anywhere,
  just a single 80/20 split. (The learning rate and convergence value as stated do match cell 198.)
- **P** Cell 199 — the "NLL" axis label, and panel 3 plotting a train sum over 912 samples against a
  test sum over 228 on shared axes, so the magnitudes aren't comparable.
- **T** "a lot variance"; "log likelyhood"; "indicies"; `\delta` where `\partial` is intended;
  a stray class subscript on σ; misplaced paren in `Pr(y=k | x⁽ⁱ⁾), W)` (twice).

**Verified correct:** the implemented gradient matches finite differences to 3.0e-10; softmax
normalizes over the class axis with rows summing to 1; the SGD sign and scaling are right; end to
end on synthetic data it matches sklearn's `LogisticRegression` (train error 0.388 vs 0.390, cosine
similarity 0.998 between the weight matrices); one-hot encoding is consistent with the loss; train
and test are transformed identically; cell 200's epoch indexing has no off-by-one.

## Deep Neural Networks (202–216)

**Backpropagation is exactly correct** — central-difference checks on your verbatim functions give
worst relative errors of 8.12e-09, 6.52e-09, 4.79e-10 and 5.77e-10 across four architectures, every
`dW` and `db`, every layer. No gradient bug exists in this section.

- **M** Cells 205, 211 — train error is an **epoch-average** over 912 different parameter vectors
  while test error is an **end-of-epoch snapshot**, and the two are plotted and compared as if
  identical. This is why cell 208 reports test (0.1096) *better* than train (0.1425) without comment,
  and why the train curve is smooth while test is jagged — which cell 215 blames on the test set
  being "more sensitive to randomness". Recording a true snapshot train error makes it just as
  jagged (e.g. epoch 12: online 0.441 vs snapshot 0.123).
- **M** Cell 203 — `Logistic_loss(derivative=True)` computes `(A−Y)/(A(1−A))`, which is `0/0` → NaN
  on saturation; `backward_prop` then multiplies the denominator back in, so the cancellation only
  works if the division doesn't overflow first. Verified: returns `[nan, 1e-300, −0.5, 0.5]` where
  `A−Y` is `[0, 1e-300, −0.5, 0.5]`. **This is exactly the root cause cell 216's optional question
  asks you to dig out** — cancel it algebraically to `A − Y`.
- **m** Cells 207, 213 — `plt.plot(x, train, test)` is the same format-string misuse as cell 199. It
  happens to be harmless here because `epochs_span = np.arange(epochs)` matches the implicit x.
- **m** Cell 206 — `layer_dim[1:1] = intermediate_layers` mutates in place; re-running the cell alone
  gives `[2914, 40, 30, 40, 30, 5]`.
- **m** Plots use 0-based epochs while the printouts say "At epochs 1…50", so plotted epoch n is
  printout epoch n+1.
- **P** Cell 209 — "the DNN is slightly better [than logistic regression]" is not supported. The
  0.1096 is the **minimum of the whole noisy tail**; recovering the curve from the stored PNG gives a
  mean over the last 10 epochs of **0.167**, versus logistic regression's 0.136. The DNN's typical
  late test error is *worse*; the honest conclusion is that they're within run-to-run noise.
- **P** Cell 215 — "much worse" overstates it: the quoted 0.421 is the tail's **worst** point
  (last-10 mean 0.267, min 0.154). ReLU genuinely is worse here, but ~0.27 vs ~0.17, not 0.42 vs 0.11.
- **P** Cell 215 — "reached some kind on convergence around epoch 30" is contradicted by cell 212's
  own output: train cost keeps falling 0.204 (ep 30) → 0.183 → 0.166 → 0.159 (ep 48).
- **P** Cell 215 — "more efficient than the other two" contradicts the next clause ("30 seconds per
  epoch, ridiculously high"), and no convergence epoch is established for either model.
- **P** Cell 212 — the comment "the above train_NN requires this layer_dim to have size = 3" is
  **false**; the functions are general in depth and cell 206 itself passes size 4.
- **T** "a error rate", "aas well as", "some kind on convergence", "propogation" throughout;
  `backward_prop_combo`'s docstring says it does the **forward** step (copy-paste), as does
  `train_NN_combo`'s.

**The "ReLU is much worse" lead — investigated, not a bug.** Init is Xavier rather than He (a factor
of √2 in variance, not a failure mode); the random *bias* at std 1 is about twice the input signal at
init, which slows early learning; but the measured active fraction is **0.498 at init and 0.477 after
training** — no dying-ReLU collapse. On an easy surrogate the same ReLU/1024 net reaches 0.000 test
error. The correct diagnosis is 3M parameters on 912 training samples with plain per-sample SGD,
read at a single noisy instant.

**Also verified correct:** activation derivatives applied to the pre-activation Z; chain rule
composed in the right order; weight gradient formed with the correct layer's input; bias gradients
right for batch size 1; update sign is descent; weights randomly initialized (no zero-init bug) and
`np.random.seed` correctly matches the `np.random.*` calls; loss averaging consistent with the
learning rate; `onehot` correct; the architectures match the prose.

---

# Cross-cutting — fix once, everywhere

**The notebook cannot be run top to bottom.** At least four independent breaks: cell 72
(`best_lam_rbf` undefined), cell 106 (`datasets` import commented out), cell 166 (`random.choice`
with numpy kwargs), and cells 67/69 (`IndexError` from the `[0]` indexing, plus `np.infty` removed
in NumPy 2.0). Worth deciding whether the portfolio version should be re-runnable at all.

**Stored outputs that don't match their source** — cells 31 (fixed), 65, 81, 92, 129, 130, 163, and
the SVM definitions cell. In several cases the executed version isn't recoverable. If you want the
document to be trustworthy, the fix is a clean top-to-bottom run after the code fixes land.

**`plt.plot(x, a, b)` format-string misuse** — cells 199, 207, 213. Harmless in two of the three.

**Missing seeds** — GMM (cell 167) and the SVM/K-medians runs set none, so nothing is reproducible.

**`\delta` where `\partial` is meant** — cells 155, 156, 157, 195.

**"implimentation"** — cells 107, 108 (in stored *output*), 109, 118, 129, 99. Also "propogation"
(200, 207), "v.s" (60, 118, and elsewhere), "guarenteed" (115), "indicies" (91, 193).

**Data needed to re-run** — currently absent locally and requiring network: `features.txt`/
`target.txt` (SVM), `nyc_taxi_rides.csv` (K-medians), `faces.csv` (PCA), and LFW via
`fetch_lfw_people` (softmax, DNN). MNIST is cached at `~/.keras/datasets/mnist.npz`, but `keras`
itself isn't installed — cells 14, 85 and 126 need `np.load` instead. The crime and prostate data
are present.

# What could not be verified

- Decision trees, sub-questions 1 and 2 — the figures are unreachable *and* the answer cells don't
  render.
- Exact stored numbers for any section whose dataset is missing (SVM, K-medians, PCA, softmax, DNN).
  Findings for those come from finite-difference gradient checks, synthetic reproduction, surrogate
  datasets, and arithmetic back-solved from the stored outputs — each reviewer flagged which.
- The DNN curve values in cells 209/215 are pixel-recovered from the stored PNGs (±~0.01), calibrated
  against the exactly-known printed finals. The qualitative conclusions are far larger than that
  error bar.
- Tuning claims that the notebook records only one run of ("going to a higher epoch value seemed to
  increase the error rate", "doing more than 2 layers seemed to increase the error", "after fiddling
  with the neuron count").
