# Machine Learning pack audit — 30 cards

All 30 cards read in full (20 algorithm cards with pseudocode, 10 concept cards).
Findings are ordered within each section by severity.

## Pseudocode errors

- **Convolutional Neural Network** — `+ A[r*s + di, c*s + dj, ch] * W[f][di, dj, ch]` with `for di in 1..k, dj in 1..k` — off-by-one. With 1-based indices, output position `r` reads input rows `r*s + 1 … r*s + k`, so output position 1 skips input row 1 entirely and the last output position indexes past the end of `A`. → should be `A[(r-1)*s + di, (c-1)*s + dj, ch]` (or 0-based `r*s + di`, with `di in 0..k-1`).

- **Convolutional Neural Network** — `if layer is POOL, window 2 x 2, stride 2:` / `A[r, c, f] <- max over that window` — two problems in one line: `r`, `c`, `f` are never bound by any loop (every other layer branch operates on the whole tensor), and it writes into `A` while the window is still reading from `A`, so pooled values would be read back as inputs to later windows. → needs an explicit output tensor and a loop over output positions, e.g. `P[r, c, f] <- max over A[2r-1..2r, 2c-1..2c, f]`, then `A <- P`.

- **Gradient Boosting (XGBoost)** — `gain <- G_L^2/(H_L+lambda) + G_R^2/(H_R+lambda) - (G_L+G_R)^2/(H_L+H_R+lambda) - gamma` — the ½ factor from Eq. 7 of Chen & Guestrin is missing. Because `gamma` is subtracted *outside* the bracket, dropping the ½ silently doubles every gain relative to the split penalty, so the `stop if best gain <= 0` test prunes differently from real XGBoost. → `gain <- 0.5 * ( G_L^2/(H_L+lambda) + G_R^2/(H_R+lambda) - (G_L+G_R)^2/(H_L+H_R+lambda) ) - gamma`.

- **Gradient Boosting (XGBoost)** — `for each candidate (feature, threshold) at that node:` … `gain <- …` followed by `split on the best gain` — `gain` is overwritten each iteration and no best-so-far is ever kept, so "the best gain" refers to a value the loop discarded. Every other card in the pack (Decision Tree, Random Forest) tracks `best_gain`/`best_split` explicitly. → add `if gain > best_gain: best_gain <- gain; best_split <- (feature, threshold)` inside the loop.

- **Support Vector Machine** — `new_j <- clip new_j into [0, C]     # box constraint` followed by `# keep sum_k alpha_k * y_k = 0 by moving the pair together` — the comment claims an invariant the code does not maintain. SMO must clip `alpha_j` to the pair-specific bounds `[L, H]` (derived from `y_i`, `y_j`, `C` and the current pair), not to `[0, C]`; clipping to the raw box lets the paired update push `alpha_i` outside `[0, C]` and breaks the equality constraint. → `new_j <- clip new_j into [L, H]` with L, H computed from the pair, and re-check `new_i` against `[0, C]`.

- **Gradient Descent** — `if change_in_loss < tolerance:` / `stop` — `change_in_loss` is never assigned anywhere in the block. `loss` is reassigned inside the minibatch loop and never accumulated at epoch level, so there is nothing to difference against. → track `epoch_loss` across minibatches and compare with the previous epoch's, or drop the clause and rely on `max_epochs`.

- **Transformer (Attention)** — `function multi_head_attention(X, h):` … `outputs[i] <- scaled_dot_product_attention(Q_i, K_i, V_i, mask)` — `mask` is passed but is not a parameter of `multi_head_attention` and is never assigned in its body. → `function multi_head_attention(X, h, mask)`.

- **Logistic Regression** — `w[j] <- w[j] - a * (grad_w[j] / N + lambda * w[j])` — `lambda` is used but never introduced; the input line reads `# inputs: N examples (x, y) with y in {0,1}, step size a, passes T`. This matters because the closing paragraph calls lambda "the knob that matters most". → add `regularisation strength lambda` to the inputs line.

- **Principal Component Analysis** — `X_approx <- Z * transpose(W) + mu` — if the "optional but usual" standardisation step above it was taken, the reconstruction must be re-scaled by the per-column standard deviation before `mu` is added back, otherwise `X_approx` is not in the units of the original data. → either return `sigma` and write `X_approx <- (Z * transpose(W)) * sigma + mu`, or say the reconstruction assumes centring only. (Also: `X_approx` is computed and then not returned — `return W, Z, mu, explained`.)

- **Backpropagation** — `grad_W_l <- d . transpose(a_(l-1))` / `grad_b_l <- d` inside `for each minibatch (X, Y)` — the loop is explicitly over a minibatch, but the gradients are never summed or averaged over the examples in it; `grad_b_l <- d` in particular assigns rather than reduces. → `grad_b_l <- sum over batch of d`, and divide both by the batch size (or state that the notation is per-example).

- **k-Nearest Neighbors** — `where w(d) = 1, or 1/d to weight by closeness` — divides by zero whenever the query coincides exactly with a stored point, which is the common case for deduplication and retrieval, two uses the card names. → `1/(d + eps)`, or return the exact-match label directly.

- **Naive Bayes** — training uses the multinomial estimator `(count(f in class c) + 1) / (total_feature_count(c) + size(vocabulary))` but prediction iterates `for each feature f present in example` and adds each log-likelihood once. That is the Bernoulli scoring rule applied to multinomial parameters: repeated words are counted once, and absent features contribute nothing rather than `log(1 - p)`. → either weight by `count(f in example)` (multinomial) or estimate Bernoulli parameters over documents.

- **k-Means Clustering** — `repeat until assignments stop changing (or max iterations):` combined with `if members is empty: centroids[j] <- re-seed at a far-away point` — re-seeding inside the loop mutates assignments, so the stated termination condition can be defeated indefinitely; only the `max iterations` escape saves it. Minor, but worth a word.

- **Generative Adversarial Network** — `repeat until samples look good:` — no termination condition a reader could implement. Honest about GAN practice, but every other card gives a concrete stopping rule. → "for N iterations, monitoring samples" or similar.

## Factual / attribution errors

- **Support Vector Machine** — "Vapnik and Chervonenkis described the linear maximum-margin classifier in 1963" → 1964. The 1963 paper is Vapnik & **Lerner**, "Pattern recognition using generalized portrait method"; the Vapnik & Chervonenkis maximum-margin paper is 1964. (Verified: Wikipedia — Support vector machine, History: "The original SVM algorithm was invented by Vladimir N. Vapnik and Alexey Ya. Chervonenkis in 1964.") Either move the date to 1964 or change the second name to Lerner.

- **Gradient Boosting (XGBoost)** — "XGBoost is the 2016 implementation by Tianqi Chen and Carlos Guestrin" → the *paper* is 2016 (KDD); the implementation dates from 2014 and was already widely used in Kaggle competitions before the paper. "the implementation … described in their 2016 paper" is the accurate phrasing.

- **Backpropagation** — "one backward pass costs roughly one forward pass" → the standard figure is roughly **twice** a forward pass (one multiply-accumulate for the input gradient, one for the weight gradient). The claim as stated understates it by 2x. "roughly the same order as a forward pass" would be safe.

Everything else checked out. Spot-verified and correct: Legendre 1805 / Gauss 1809 / Galton 1880s; Lloyd 1957–1982, Steinhaus 1956, Forgy 1965, MacQueen 1967; Linnainmaa 1970, Werbos 1974, Kelley/Bryson ~1960, RHW 1986; Sohl-Dickstein 2015, Song & Ermon 2019, Ho/Jain/Abbeel 2020; Verhulst, Berkson 1944 (coined "logit"), Cox 1958, Walker & Duncan 1967; CART 1984, Quinlan ID3/C4.5, Morgan & Sonquist 1963; Pearson 1901, Hotelling 1933; Hochreiter 1991 thesis, H&S 1997, Gers/Schmidhuber/Cummins 1999–2000; Schulman TRPO 2015 / PPO 2017; Boser-Guyon-Vapnik 1992, Cortes & Vapnik 1995; Schapire/Freund, AdaBoost 1995, Friedman 1999–2001; Fix & Hodges 1951, Cover & Hart 1967; Hubel & Wiesel, Fukushima 1980, LeCun 1989/1998, AlexNet 2012; Kingma & Ba 2014/ICLR15, Duchi 2011, Tieleman & Hinton 2012 lecture, Reddi 2018, Loshchilov & Hutter 2017; Bayes 1763, Laplace; Cauchy 1847, Hadamard 1907, Robbins & Monro 1951, Kiefer & Wolfowitz 1952; Watkins 1989/1992, Bellman, Sutton 1988; Vaswani 2017, Bahdanau 2014, Luong 2015; Goodfellow 2014 and the Schmidhuber 1992 dispute; Geman/Bienenstock/Doursat 1992; Tikhonov, Hoerl & Kennard 1970, Tibshirani 1996; Stone 1974, Geisser 1975, Kohavi 1995; Wald; Cleverdon/Cranfield, Provost & Fawcett; Firth 1957, LSA ~1990, Bengio 2003, Mikolov 2013; Pratt, Caruana, Yosinski 2014, ULMFiT/ELMo/BERT 2018; Devlin 2018, SimCLR/MoCo 2020; Hestness 2017, Kaplan 2020, Hoffmann 2022. Ho 1995 "random subspace method" also checks out against the Random forest article, so leave it.

## Overstated or misleading claims

- **Gradient Descent** — "Every model trained today, from logistic regression to trillion-parameter language models, is fitted by some descendant of this." — flatly false, and contradicted by this pack's own cards: Random Forest, Decision Tree, k-Means, k-NN, Naive Bayes and PCA are none of them fitted by gradient descent. → "Every neural network trained today … is fitted by some descendant of this" or "most models with differentiable parameters".

- **Loss Functions** — "The catch is that a loss must be smooth and differentiable, while the things you care about … usually are not" — the same paragraph has already named two losses that are neither: "absolute error targets the median" and "the hinge loss arrived with support vector machines". Subgradient methods handle both, and they are standard. → "must give you a usable gradient almost everywhere".

- **Q-Learning** — "learns the best policy even while behaving randomly to explore" — presented as unconditional, but the Watkins & Dayan guarantee requires every state-action pair be visited infinitely often and a Robbins-Monro learning-rate schedule. Under function approximation it does not hold at all, which the card's own closing paragraph then assumes ("that then needs a replay buffer and a frozen target network to stay stable"). → "can learn the optimal policy while exploring, given enough visits to every state-action pair".

- **k-Nearest Neighbors** — "Beyond a few million points, swap the exhaustive scan for a KD-tree or an approximate index such as HNSW." — KD-trees degenerate to brute force above roughly 20 dimensions, which is exactly the regime the card has just praised k-NN for ("similarity search over learned embeddings"). Offering KD-tree and HNSW as interchangeable is misleading. → "a KD-tree in low dimensions, or an approximate index such as HNSW for embeddings".

- **Precision, Recall and Class Imbalance** — "F1 summarises the pair but assumes both errors hurt equally" — F1 weights *precision and recall* equally, which is not the same as false positives and false negatives costing the same; F1 ignores true negatives entirely, so its symmetry is between the two rates, not the two error types. → "assumes precision and recall matter equally".

- **Linear Regression** — "Practitioners almost always add ridge or lasso regularisation" — overstated, and in tension with the preceding sentence about economists and epidemiologists reading coefficients, who overwhelmingly fit unpenalised OLS precisely so the coefficients stay interpretable. → "often add".

- **Random Forest** — "loses to tuned gradient boosting" — stated as an unconditional outcome. True on average on tabular benchmarks, not true per dataset, and the card offers no hedge. → "usually loses to well-tuned gradient boosting".

- **Principal Component Analysis** — "It is cheap, deterministic, and has no training loop to babysit." — "cheap" is doing a lot of work for an O(d³) eigendecomposition on wide data (the closing paragraph quietly concedes this by recommending SVD instead), and PCA is deterministic only up to the sign of each component, which trips people comparing runs. Worth one qualifying clause.

- **Decision Tree (CART)** — "pruning back a grown tree beats stopping early" — the CART position, presented as settled fact. It is the better default, not a universal result. → "usually beats".

## Prose issues

- **Convolutional Neural Network** — "reading handwriting and printed text satellite imagery, visual defect inspection on production lines" — a missing comma runs two list items together; the sentence does not parse on first read. → "reading handwriting and printed text, satellite imagery, visual defect inspection…".

- **Stock closing sentence, seven cards.** Almost every algorithm card ends its post-pseudocode paragraph with the same sentence shape — "X is the one hyperparameter that matters":
  - Random Forest: "The features-per-split count is the hyperparameter that actually matters"
  - Logistic Regression: "The knob that matters most is the regularisation strength lambda"
  - Decision Tree: "Depth and minimum leaf size are the settings that decide everything"
  - Support Vector Machine: "The two knobs that matter are C … and the RBF kernel width gamma"
  - Adam: "The learning rate is still the hyperparameter that matters most"
  - Naive Bayes: "The smoothing constant is the one hyperparameter that really matters"
  - Gradient Descent: "The learning rate is the make-or-break hyperparameter"
  - (Embeddings does it too: "Dimension is the main dial")
  Read in sequence this is the single most visible tic in the pack. Vary at least half of them — some cards' real advice is not about a hyperparameter at all.

- **"genuinely" in the Discovered-by lines, five cards.** "Credit is genuinely shared" (Linear Regression), "Credit here is genuinely incremental" (Logistic Regression), "Credit is layered rather than single" (SVM), "attribution is genuinely diffuse" (Naive Bayes), "Credit is genuinely layered" (Gradient Descent). The shared-credit framing is the point of these cards, but the word "genuinely" is doing defensive work in all five and should survive in at most one.

- **"workhorse", four cards.** "The reliable workhorse for messy tabular data" (Random Forest), "The workhorse for continuous-control robotics" (PPO), "The workhorse classifier of the late 1990s and 2000s" (SVM), "The workhorse of spam filtering" (Naive Bayes).

- **"The default", five cards.** "The default first model" (Linear Regression), "the default for speech recognition" (LSTM), "The default choice for structured, tabular prediction" (Gradient Boosting), "The default optimiser for training deep networks" (Adam), "Cross-entropy is the default for classification" (Loss Functions).

- **"A way to…" as a `primary` opener, four cards** — Logistic Regression ("A way to estimate the probability…"), Gradient Boosting ("A way to get accurate predictions…"), k-NN ("A way to label or predict something new…"), Q-Learning ("A way for an agent to learn…"). Plus "Reached for wherever" / "It is reached for because" / "It gets reached for because" across Decision Tree, k-Means and PPO.

- **Precision, Recall and Class Imbalance** — "Class weights and resampling help training; applying them to your validation set, or reporting one number for an asymmetric problem, does not." — the compound subject after the semicolon ("applying them … or reporting one number …") does not agree with the singular "does not", and the parallel with "help training" collapses. Needs splitting into two sentences.

- **Logistic Regression** — `primary`: "A way to estimate the probability that something belongs to one of two classes, such as spam or not spam, from a handful of measurements." — "a handful of measurements" is wrong in spirit; logistic regression is routinely fitted on thousands of features (the card's own text names text classification and ad click prediction). Drop the clause.

- **Loss Functions** — `primary`: "The single number training minimises; choosing it is choosing what the model is rewarded for, and therefore what it will learn to do." — mixes minimising with rewarding in one clause, and the semicolon splices a noun phrase to a full sentence. Pick one frame.

- **Transformer (Attention)** — `primary`: "A design that lets a model read a whole sequence at once and decide, for each piece, which other pieces matter most to understanding it." — describes the mechanism, not what the thing is for. Same issue, milder, in **k-Means** ("by repeatedly nudging each group's centre toward the points nearest to it") and **Overfitting and Underfitting** ("The two ways a model fails to generalise…", a definition rather than a purpose). Per the pack's own convention, `primary` should say what it is FOR.

- **Neural Scaling Laws** — "The caveats matter." — a bare two-word fragment introducing a semicolon-chained list of four caveats. Fold it into the list sentence.

- **Embeddings** — "Corpus bias also becomes geometric bias, faithfully preserved." — the trailing participle has no clear subject and the sentence lands as an aphorism rather than a point. → "Bias in the corpus is preserved as geometry in the vectors."

- **Used-for template.** All 20 algorithm cards run the identical three-move paragraph: list of applications → why people reach for it → "It is bad at / poor on / weak at …". The rhythm is legible enough that reading five in a row feels mechanical. Breaking the order on a handful of cards would help more than any single-sentence edit.

## Clean

Diffusion Models · Long Short-Term Memory (LSTM) · Proximal Policy Optimization (PPO) · Adam Optimizer · Overfitting and Underfitting · Regularisation (L1 and L2) · Cross-Validation and the Train/Validation/Test Split · Feature Engineering and Feature Scaling · Transfer Learning and Fine-Tuning · Self-Supervised Learning

(These have no card-specific finding; they still participate in the cross-cutting stock-phrasing issues listed above.)
