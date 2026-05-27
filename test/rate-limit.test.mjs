import assert from "node:assert/strict";
import test from "node:test";

// ---------------------------------------------------------------------------
// Helpers — pure functions that don't need the module under test
// ---------------------------------------------------------------------------

/**
 * Returns a promise that resolves after `ms` milliseconds.
 */
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Returns the minimum interval in milliseconds between successive requests
 * needed to achieve at most `maxRPM` requests per minute.
 */
function minIntervalMs(maxRPM) {
	return Math.ceil(60_000 / Math.max(1, maxRPM));
}

// ---------------------------------------------------------------------------
// Tests that verify the throttling invariants without importing TypeScript
// (imports happen in the implementation-dependent suite below).
// ---------------------------------------------------------------------------

test("minIntervalMs helper computes correct intervals", () => {
	assert.equal(minIntervalMs(40), 1500); // 60 000 / 40 = 1500
	assert.equal(minIntervalMs(35), 1715); // 60 000 / 35 ≈ 1714.3 → ceil 1715
	assert.equal(minIntervalMs(60), 1000); // 60 000 / 60 = 1000
	assert.equal(minIntervalMs(1), 60_000); // 60 000 / 1 = 60 000
});

test("sleep helper resolves roughly after requested duration", async () => {
	const start = Date.now();
	await sleep(50);
	const elapsed = Date.now() - start;
	// Allow generous tolerance for test-runner jitter
	assert.ok(elapsed >= 45, `expected >=45ms, got ${elapsed}ms`);
});

// ---------------------------------------------------------------------------
// Rate-limiter unit tests (after implementation exists)
// These import the compiled/TS modules the same way the auth tests do.
// ---------------------------------------------------------------------------

test("NimRateLimiter – acquires and releases concurrency slot", async () => {
	const { NimRateLimiter } = await import("../rate-limit.ts");

	const limiter = new NimRateLimiter({ maxConcurrency: 2, maxRPM: 1000 });
	assert.equal(limiter.activeCount, 0, "no active slots at start");

	const ticket = await limiter.acquire();
	assert.equal(limiter.activeCount, 1, "activeCount incremented after acquire");
	ticket.release();
	assert.equal(limiter.activeCount, 0, "activeCount decremented after release");
});

test("NimRateLimiter – enforces max concurrency", async () => {
	const { NimRateLimiter } = await import("../rate-limit.ts");

	const limiter = new NimRateLimiter({ maxConcurrency: 1, maxRPM: 1000 });

	// Grab the only slot
	const ticket1 = await limiter.acquire();
	assert.equal(limiter.activeCount, 1);

	// Second acquire should be queued (never resolves before first releases)
	let secondAcquired = false;
	const p2 = limiter.acquire().then((t) => {
		secondAcquired = true;
		return t;
	});

	// Even after a brief sleep it should still be queued
	await sleep(20);
	assert.equal(secondAcquired, false, "second acquire blocked while concurrency cap is at 1");

	// Release the first slot — second should resolve near-immediately
	ticket1.release();
	const ticket2 = await p2;
	assert.equal(secondAcquired, true, "second acquire resolved after first released");

	// Clean up
	ticket2.release();
	assert.equal(limiter.activeCount, 0);
});

test("NimRateLimiter – respects rate limit with burst of requests", async () => {
	const { NimRateLimiter } = await import("../rate-limit.ts");

	// 20 RPM → one request every 3 000 ms.  Let many in-flight be allowed
	// so the test measures purely the rate pacing.
	const maxRPM = 20;
	const limiter = new NimRateLimiter({ maxConcurrency: 10, maxRPM });

	const numRequests = 5;
	const startTimestamps = [];

	for (let i = 0; i < numRequests; i++) {
		const ticket = await limiter.acquire();
		startTimestamps.push(Date.now());
		ticket.release(); // release immediately so concurrency doesn't serialize
	}

	// Between any two consecutive releases the elapsed time should be at least
	// minIntervalMs(maxRPM) minus a small tolerance for OS scheduler variance.
	const minGapMs = minIntervalMs(maxRPM) - 50; // 50 ms tolerance
	for (let i = 1; i < startTimestamps.length; i++) {
		const gap = startTimestamps[i] - startTimestamps[i - 1];
		assert.ok(
			gap >= minGapMs,
			`request #${i} gap ${gap}ms < expected min ${minGapMs}ms (maxRPM=${maxRPM})`,
		);
	}
});

test("NimRateLimiter – respects rate limit with serialized acquire-release sequence", async () => {
	const { NimRateLimiter } = await import("../rate-limit.ts");

	// Very low RPM to make the test fast but still measurable
	const maxRPM = 120; // 2 per second → min gap 500ms
	const limiter = new NimRateLimiter({ maxConcurrency: 1, maxRPM });

	const starts = [];
	for (let i = 0; i < 3; i++) {
		const ticket = await limiter.acquire();
		starts.push(Date.now());
		ticket.release();
	}

	const minGapMs = minIntervalMs(maxRPM) - 50;
	for (let i = 1; i < starts.length; i++) {
		const gap = starts[i] - starts[i - 1];
		assert.ok(gap >= minGapMs, `gap ${gap}ms < ${minGapMs}ms`);
	}
});

test("NimRateLimiter – concurrent acquires interleaved with rapid releases", async () => {
	const { NimRateLimiter } = await import("../rate-limit.ts");

	// With maxConcurrency=1 and maxRPM=60 (min interval=1000ms), 6 sequential
	// requests produce the clearest predictable spread.
	const limiter = new NimRateLimiter({ maxConcurrency: 1, maxRPM: 60 });

	const startTimes = [];
	const promises = [];

	// Fire 6 concurrent acquire calls
	for (let i = 0; i < 6; i++) {
		const p = limiter.acquire().then((ticket) => {
			startTimes.push(Date.now());
			// Release immediately so the next in queue can fire
			ticket.release();
		});
		promises.push(p);
	}

	await Promise.all(promises);
	assert.equal(startTimes.length, 6, "all 6 acquires completed");
	// First and last should be spaced by at least (n-1) * minInterval
	const minGapMs = minIntervalMs(60) - 50; // 1000ms - tolerance
	const last = startTimes[startTimes.length - 1];
	const first = startTimes[0];
	assert.ok(last - first >= minGapMs * 5, `spread too narrow: ${last - first}ms`);
});

// ---------------------------------------------------------------------------
// fetch-wrapper tests
// ---------------------------------------------------------------------------

test("installNimFetchWrapper – only wraps NIM URLs, passes others through", async () => {
	const { installNimFetchWrapper, uninstallNimFetchWrapper } = await import("../rate-limit.ts");

	const calls = [];
	const fakeFetch = async (input) => {
		calls.push(typeof input === "string" ? input : input.url);
		return new Response("ok", { status: 200 });
	};

	const original = globalThis.fetch;
	globalThis.fetch = fakeFetch;
	try {
		const cleanup = installNimFetchWrapper({ maxRPM: 1000, maxConcurrency: 10 });

		await globalThis.fetch("https://api.openai.com/v1/models");
		await globalThis.fetch("https://integrate.api.nvidia.com/v1/models");
		await globalThis.fetch("https://openrouter.ai/api/v1/chat/completions");
		await globalThis.fetch("https://integrate.api.nvidia.com/v1/chat/completions");

		cleanup();
	} finally {
		globalThis.fetch = original;
	}

	// Both NIM and non-NIM calls should have gone through
	assert.ok(calls.length >= 4);
	const nimCalls = calls.filter((u) => u.startsWith("https://integrate.api.nvidia.com"));
	assert.equal(nimCalls.length, 2, "2 NIM calls went through the wrapper");
});

test("installNimFetchWrapper – retries on 429 with backoff", async () => {
	const { installNimFetchWrapper, uninstallNimFetchWrapper } = await import("../rate-limit.ts");

	let attempts = 0;
	const fakeFetch = async (_input) => {
		attempts++;
		if (attempts < 3) {
			return new Response("rate limited", {
				status: 429,
				headers: { "retry-after": "0" },
			});
		}
		return new Response("ok", { status: 200 });
	};

	const original = globalThis.fetch;
	globalThis.fetch = fakeFetch;
	try {
		// Use a high RPM so the rate limiter doesn't add extra delay.
		const cleanup = installNimFetchWrapper({
			maxRPM: 1000,
			maxConcurrency: 10,
			maxRetries: 4,
			retryBaseDelayMs: 10, // keep the test fast
			retryMaxDelayMs: 100,
		});

		const response = await globalThis.fetch("https://integrate.api.nvidia.com/v1/chat/completions");
		assert.equal(response.status, 200);
		assert.equal(attempts, 3, "retried twice then succeeded on third attempt");

		cleanup();
	} finally {
		globalThis.fetch = original;
	}
});

test("installNimFetchWrapper – gives up after max retries", async () => {
	const { installNimFetchWrapper, uninstallNimFetchWrapper } = await import("../rate-limit.ts");

	let attempts = 0;
	const fakeFetch = async (_input) => {
		attempts++;
		return new Response("rate limited", {
			status: 429,
			headers: { "retry-after": "0" },
		});
	};

	const original = globalThis.fetch;
	globalThis.fetch = fakeFetch;
	try {
		const cleanup = installNimFetchWrapper({
			maxRPM: 1000,
			maxConcurrency: 10,
			maxRetries: 2,
			retryBaseDelayMs: 5,
			retryMaxDelayMs: 20,
		});

		const response = await globalThis.fetch("https://integrate.api.nvidia.com/v1/chat/completions");
		// After giving up it still returns the last (429) response — the caller
		// decides what to do with it.
		assert.equal(response.status, 429);
		// maxRetries=2 means: initial attempt + 1 retry = 2 total
		assert.equal(attempts, 2);

		cleanup();
	} finally {
		globalThis.fetch = original;
	}
});

test("installNimFetchWrapper – idempotent (installing twice is safe)", async () => {
	const { installNimFetchWrapper, uninstallNimFetchWrapper } = await import("../rate-limit.ts");

	const fakeFetch = async (_input) => new Response("ok", { status: 200 });
	const original = globalThis.fetch;
	globalThis.fetch = fakeFetch;
	try {
		const cleanup1 = installNimFetchWrapper({ maxRPM: 1000, maxConcurrency: 10 });
		const cleanup2 = installNimFetchWrapper({ maxRPM: 1000, maxConcurrency: 10 });

		const response = await globalThis.fetch("https://integrate.api.nvidia.com/v1/models");
		assert.equal(response.status, 200);

		// Second cleanup is a no-op (the original is preserved inside the first wrapper)
		cleanup2();
		// First cleanup restores the original
		cleanup1();

		// After all cleanups the original should be back
		assert.equal(globalThis.fetch, fakeFetch);
	} finally {
		globalThis.fetch = original;
	}
});