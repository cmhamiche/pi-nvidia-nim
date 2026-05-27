/**
 * Rate limiter and fetch wrapper for NVIDIA NIM API (integrate.api.nvidia.com).
 *
 * NVIDIA NIM free-tier preview keys are subject to (at least):
 *   - 40 requests per minute (soft limit)
 *   - 5 concurrent in-flight requests (hard limit)
 *
 * This module provides a sliding-window rate limiter + concurrency cap and
 * a scoped ``globalThis.fetch`` wrapper that applies them transparently
 * **only** to requests targeting ``https://integrate.api.nvidia.com``.
 * All other providers are passed through untouched.
 *
 * The wrapper also retries HTTP 429 responses with exponential backoff
 * (respecting the ``Retry-After`` header when present).
 *
 * Usage (typically called once from the extension's init function):
 *
 *   import { installNimFetchWrapper } from "./rate-limit.ts";
 *   const cleanup = installNimFetchWrapper();
 *   // … later, if ever needed …
 *   cleanup();
 */

// ---------------------------------------------------------------------------
// Configuration from environment
// ---------------------------------------------------------------------------

const NIM_BASE_URL = "https://integrate.api.nvidia.com";

function envInt(name: string, fallback: number): number {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return fallback;
	const n = Number.parseInt(raw, 10);
	return Number.isFinite(n) && n > 0 ? n : fallback;
}

function envFloat(name: string, fallback: number): number {
	const raw = process.env[name];
	if (raw === undefined || raw === "") return fallback;
	const n = Number.parseFloat(raw);
	return Number.isFinite(n) && n > 0 ? n : fallback;
}

// ---------------------------------------------------------------------------
// Sliding-window rate limiter + concurrency semaphore
// ---------------------------------------------------------------------------

export interface Ticket {
	/** Release the slot back to the limiter. Idempotent. */
	release(): void;
}

export interface NimRateLimiterOptions {
	/** Maximum in-flight (acquired-but-not-released) requests. Default 5. */
	maxConcurrency: number;
	/** Maximum requests per minute (sliding window). Default 35. */
	maxRPM: number;
}

export class NimRateLimiter {
	// Timestamps (ms) of recent request starts, kept sorted oldest→newest.
	private _requestStarts: number[] = [];

	// Count of active (acquired-but-not-released) slots.
	private _active = 0;

	// Queue of pending acquires waiting for a concurrency slot.
	private _concurrencyQueue: Array<{ resolve: () => void }> = [];

	// Time (ms) at which the next request is permitted to start. Used for
	// min-interval pacing so that concurrent acquire() calls back off in a
	// deterministic order regardless of who wins the race.
	private _nextScheduledStart = 0;

	private _windowMs: number;
	private _maxConcurrency: number;
	private _maxRPM: number;
	private _minIntervalMs: number;

	constructor(opts: NimRateLimiterOptions) {
		this._maxConcurrency = opts.maxConcurrency;
		this._maxRPM = opts.maxRPM;
		this._windowMs = 60_000;
		this._minIntervalMs = opts.maxRPM > 0 ? Math.ceil(60_000 / opts.maxRPM) : 0;
	}

	/** Number of currently active (acquired but not released) slots. */
	get activeCount(): number {
		return this._active;
	}

	/**
	 * Acquire a ticket.  Blocks until both the rate limit and the concurrency
	 * cap allow the request to proceed.  Call ``ticket.release()`` when the
	 * HTTP request completes (headers received) to free the concurrency slot.
	 *
	 * Two guards are enforced:
	 *   1. **Sliding window**  — no more than ``maxRPM`` starts in the last 60 s.
	 *   2. **Min interval**    — time between starts is at least ``60000/maxRPM``.
	 *
	 * The min-interval guard prevents bursts and keeps the traffic profile
	 * smooth, which is important for services that assert hard rate limits
	 * without explicit 429 warnings.
	 */
	async acquire(): Promise<Ticket> {
		// --- 1. Sliding window guard ---
		while (true) {
			const now = Date.now();
			this._requestStarts = this._requestStarts.filter((t) => now - t < this._windowMs);
			if (this._requestStarts.length < this._maxRPM) break;

			const oldest = this._requestStarts[0];
			const waitMs = Math.max(1, this._windowMs - (now - oldest) + 50);
			await sleep(waitMs);
		}

		// --- 2. Min interval (anti-burst) guard ---
		// Compute the earliest time THIS call may start, then reserve the
	 // immediately following slot for the next caller. This serialises the
		// start times even when many acquire() calls are racing.
		const now = Date.now();
		const myScheduledStart = Math.max(now, this._nextScheduledStart);
		this._nextScheduledStart = myScheduledStart + this._minIntervalMs;

		if (myScheduledStart > now) {
			await sleep(myScheduledStart - now);
		}

		// --- 3. Concurrency guard ---
		if (this._active >= this._maxConcurrency) {
			await new Promise<void>((resolve) => {
				this._concurrencyQueue.push({ resolve });
			});
		}

		this._active++;
		this._requestStarts.push(Date.now());

		let released = false;
		const release = () => {
			if (released) return;
			released = true;
			this._active--;
			// Notify the next waiter in the concurrency queue
			const next = this._concurrencyQueue.shift();
			if (next) next.resolve();
		};

		return { release };
	}
}

// ---------------------------------------------------------------------------
// globalThis.fetch wrapper
// ---------------------------------------------------------------------------

export interface NimFetchWrapperOptions {
	/** RPM cap (sliding window). Default pulled from NVIDIA_NIM_MAX_RPM env or 35. */
	maxRPM?: number;
	/** Maximum in-flight requests. Default pulled from NVIDIA_NIM_MAX_CONCURRENCY env or 5. */
	maxConcurrency?: number;
	/** How many times to retry a 429 (total attempts = maxRetries). Default: NVIDIA_NIM_MAX_RETRIES or 3. */
	maxRetries?: number;
	/** Base backoff delay for first retry (ms). Default: NVIDIA_NIM_RETRY_BASE_DELAY_MS or 2000. */
	retryBaseDelayMs?: number;
	/** Maximum backoff delay for retries (ms). Default: NVIDIA_NIM_RETRY_MAX_DELAY_MS or 30000. */
	retryMaxDelayMs?: number;
}

// Store the original fetch so we can restore it and so the wrapper can
// call through to it.
let _originalFetch: typeof globalThis.fetch | null = null;
let _wrapperActive = false;

/**
 * Install a ``globalThis.fetch`` wrapper that throttles and retries requests
 * to ``https://integrate.api.nvidia.com``.  All other URLs are passed
 * through to the original ``fetch`` without any overhead.
 *
 * Returns a cleanup function that restores the original ``fetch``.
 *
 * The installation is idempotent — calling it multiple times returns the
 * same cleanup and does not add extra layers.
 */
export function installNimFetchWrapper(
	opts: NimFetchWrapperOptions = {},
): () => void {
	// Idempotent: if we're already wrapping, just return the same cleanup.
	if (_wrapperActive) return uninstallNimFetchWrapper;

	const maxRPM = opts.maxRPM ?? envFloat("NVIDIA_NIM_MAX_RPM", 35);
	const maxConcurrency = opts.maxConcurrency ?? envInt("NVIDIA_NIM_MAX_CONCURRENCY", 5);
	const maxRetries = opts.maxRetries ?? envInt("NVIDIA_NIM_MAX_RETRIES", 3);
	const retryBaseDelayMs = opts.retryBaseDelayMs ?? envFloat("NVIDIA_NIM_RETRY_BASE_DELAY_MS", 2000);
	const retryMaxDelayMs = opts.retryMaxDelayMs ?? envFloat("NVIDIA_NIM_RETRY_MAX_DELAY_MS", 30_000);

	const limiter = new NimRateLimiter({ maxRPM, maxConcurrency });

	// Save original
	_originalFetch = globalThis.fetch;

	const wrappedFetch: typeof globalThis.fetch = async (input, init) => {
		const url = typeof input === "string"
			? input
			: input instanceof URL
				? input.href
				: input.url ?? "";

		// Only intercept NIM API calls
		if (!url.startsWith(NIM_BASE_URL)) {
			return _originalFetch!(input, init);
		}

		// Acquire rate + concurrency slot
		const ticket = await limiter.acquire();

		let lastError: unknown;
		for (let attempt = 0; attempt < maxRetries; attempt++) {
			try {
				const response: Response = await _originalFetch!(input, init);

				// Release concurrency slot (headers received → request no longer
				// counts as "in-flight" from the NIM gateway perspective).
				ticket.release();

				if (response.status === 429) {
					const retryAfter = parseRetryAfter(response.headers.get("Retry-After"));
					const delay = retryAfter != null
						? retryAfter
						: Math.min(retryMaxDelayMs, retryBaseDelayMs * 2 ** attempt);
					// Add jitter: ±25 %
					const jittered = delay * (0.75 + Math.random() * 0.5);

					if (attempt < maxRetries - 1) {
						logRetry("429", attempt + 1, maxRetries, jittered, url);
						await sleep(jittered);
						continue;
					}
					// Final attempt failed — return the 429 as-is so the caller
					// (OpenAI SDK) can also apply its own retry logic.
					logRetry("429 (exhausted)", attempt + 1, maxRetries, 0, url);
					return response;
				}

				return response;
			} catch (err) {
				// Network-level errors are retried
				if (attempt < maxRetries - 1) {
					const delay = Math.min(retryMaxDelayMs, retryBaseDelayMs * 2 ** attempt);
					const jittered = delay * (0.75 + Math.random() * 0.5);
					logRetry(String(err), attempt + 1, maxRetries, jittered, url);
					await sleep(jittered);
					lastError = err;
					continue;
				}
				ticket.release();
				throw err;
			}
		}

		// Should not be reached (handled in loop), but belt-and-suspenders
		ticket.release();
		throw lastError;
	};

	globalThis.fetch = wrappedFetch;
	_wrapperActive = true;

	return uninstallNimFetchWrapper;
}

/**
 * Restore the original ``globalThis.fetch``.  Safe to call even if the
 * wrapper is not currently installed.
 */
export function uninstallNimFetchWrapper(): void {
	if (_originalFetch !== null) {
		globalThis.fetch = _originalFetch;
	}
	_originalFetch = null;
	_wrapperActive = false;
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Parse the ``Retry-After`` header value:
 *   - Seconds as a plain number.
 *   - ``0`` is treated as "immediate" (1 ms).
 *   - Returns ``null`` if the header is absent or unparseable.
 */
function parseRetryAfter(raw: string | null): number | null {
	if (raw === null || raw === undefined) return null;
	const trimmed = raw.trim();
	if (trimmed === "") return null;
	const seconds = Number.parseInt(trimmed, 10);
	if (Number.isFinite(seconds) && seconds >= 0) {
		return seconds === 0 ? 1 : seconds * 1000;
	}
	// Also try HTTP-date parsing if needed (omitted for simplicity; NIM
	// returns simple integer Retry-After values).
	return null;
}

function logRetry(
	cause: string,
	attempt: number,
	max: number,
	delayMs: number,
	url: string,
): void {
	// Strip API key from URL for safe logging
	const safeUrl = url.replace(/nvapi-[A-Za-z0-9._-]+/g, "nvapi-[redacted]");
	console.warn(
		`NIM_RETRY: cause=${cause} attempt=${attempt}/${max} backoff_ms=${Math.round(delayMs)} url=${safeUrl}`,
	);
}

/**
 * Re-export the base URL so callers (e.g. index.ts) can reuse it.
 */
export const NIM_BASE_URL_CONST = NIM_BASE_URL;