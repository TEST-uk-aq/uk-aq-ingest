/usr/local/bin/bash: warning: setlocale: LC_ALL: cannot change locale (C.UTF-8): No such file or directory
import {
  DiscoveryMessageSchema, displayTitleNeuronRates, recordQueueFailure, runSourceScan,
  configuredPublisherRssRouteKeys, scheduledGuardianRouteKeys,
  type DiscoveryMessage, type DisplayTitleAiRuntime,
} from '@uk-aq-media/discovery';
import { json, loadAuthorRules, loadSource, secretMatches } from '@uk-aq-media/core';

const NEURON_MICRO_UNITS = 1_000_000;

function configuredDisplayTitleAi(env: MediaDiscoveryEnv): DisplayTitleAiRuntime | undefined {
  const model = String(env.MEDIA_AI_MODEL).trim();
  const perRun = Number(env.MEDIA_AI_MAX_REQUESTS_PER_RUN);
  const dailyNeuronBudget = Number(env.MEDIA_AI_DAILY_NEURON_BUDGET);
  const accountFreeNeurons = Number(env.MEDIA_AI_ACCOUNT_FREE_NEURONS_PER_DAY);
  const maximumTitlesPerRequest = Number(env.MEDIA_AI_MAX_TITLES_PER_REQUEST);
  if (!/^@cf\/[a-z0-9._/-]+$/i.test(model) || model.length > 200 ||
      !Number.isSafeInteger(perRun) || perRun < 1 || perRun > 10 ||
      !Number.isSafeInteger(dailyNeuronBudget) || dailyNeuronBudget < 1 ||
      !Number.isSafeInteger(accountFreeNeurons) || accountFreeNeurons < 1 ||
      dailyNeuronBudget > accountFreeNeurons ||
      !Number.isSafeInteger(dailyNeuronBudget * NEURON_MICRO_UNITS) ||
      !Number.isSafeInteger(accountFreeNeurons * NEURON_MICRO_UNITS) ||
      !Number.isSafeInteger(maximumTitlesPerRequest) ||
      maximumTitlesPerRequest < 1 || maximumTitlesPerRequest > 5 ||
      displayTitleNeuronRates(model) === null) {
    console.warn(JSON.stringify({ event: 'display_title_ai_configuration_disabled' }));
    return undefined;
  }
  return { binding: env.AI, model, maximum_requests_per_run: perRun,
    daily_neuron_budget: dailyNeuronBudget,
    account_free_neurons_per_day: accountFreeNeurons,
    maximum_titles_per_request: maximumTitlesPerRequest };
}

async function enqueueScheduledDiscovery(env: MediaDiscoveryEnv): Promise<number> {
  try {
    // This explicit adapter allowlist prevents enabling unfinished publishers through D1 alone.
    // Request data never participates in this internally selected, bounded work list.
    const [aqn, guardian, bbc, sky, independent, carbonBrief, openaq, physOrg] = await Promise.all([
      loadSource(env.MEDIA_DB, 'air-quality-news'),
      loadSource(env.MEDIA_DB, 'the-guardian'),
      loadSource(env.MEDIA_DB, 'bbc-news'),
      loadSource(env.MEDIA_DB, 'sky-news'),
      loadSource(env.MEDIA_DB, 'the-independent'),
      loadSource(env.MEDIA_DB, 'carbon-brief'),
      loadSource(env.MEDIA_DB, 'openaq'),
      loadSource(env.MEDIA_DB, 'phys-org'),
    ]);
    const messages: DiscoveryMessage[] = [];
    if (aqn?.enabled) messages.push({ type: 'source_scan', source_key: 'air-quality-news' });
    if (guardian?.enabled) {
      const rules = await loadAuthorRules(env.MEDIA_DB, 'the-guardian');
      for (const routeKey of scheduledGuardianRouteKeys(guardian, rules)) {
        messages.push({ type: 'source_scan', source_key: 'the-guardian', route_key: routeKey });
      }
    }
    for (const source of [bbc, sky, independent, carbonBrief, openaq, physOrg]) {
      if (!source?.enabled) continue;
      for (const routeKey of configuredPublisherRssRouteKeys(source)) {
        messages.push({ type: 'source_scan', source_key: source.source_key, route_key: routeKey });
      }
    }
    if (messages.length === 0) return 0;
    await env.MEDIA_DISCOVERY_QUEUE.sendBatch(messages.map(body => ({ body })));
    return messages.length;
  } catch {
    await recordQueueFailure(env.MEDIA_DB, 'discovery_schedule_failed');
    console.error(JSON.stringify({ event: 'discovery_schedule_failed' }));
    throw new Error('discovery_schedule_failed');
  }
}

export default {
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== '/scheduler/run') return json({ error: 'not_found' }, 404);
    if (request.method !== 'POST') {
      const response = json({ error: 'method_not_allowed' }, 405);
      response.headers.set('Allow', 'POST');
      return response;
    }
    const suppliedSecret = request.headers.get('x-uk-aq-worker-http-secret');
    if (!await secretMatches(suppliedSecret, env.UK_AQ_MEDIA_WORKER_HTTP_SECRET)) {
      return json({ error: 'unauthorized' }, 401);
    }
    try {
      const queued = await enqueueScheduledDiscovery(env);
      return json({ ok: true, queued }, 202);
    } catch {
      return json({ error: 'discovery_schedule_failed' }, 503);
    }
  },
  async scheduled(_controller, env): Promise<void> {
    await enqueueScheduledDiscovery(env);
  },
  async queue(batch, env): Promise<void> {
    for (const message of batch.messages) {
      const parsed = DiscoveryMessageSchema.safeParse(message.body);
      if (!parsed.success || parsed.data.type === 'article_enrich') {
        // Not operational in this phase: never acknowledge unimplemented work as completed.
        await recordQueueFailure(env.MEDIA_DB, 'unsupported_discovery_message', message.id, message.attempts);
        console.error(JSON.stringify({ event: 'unsupported_discovery_message', message_id: message.id }));
        message.retry({ delaySeconds: 300 });
        continue;
      }
      try {
        await runSourceScan(env.MEDIA_DB, parsed.data.source_key, parsed.data.route_key,
          message.id, message.attempts, configuredDisplayTitleAi(env));
        message.ack();
      } catch {
        console.error(JSON.stringify({ event: 'discovery_message_failed', message_id: message.id }));
        message.retry({ delaySeconds: 300 });
      }
    }
  },
} satisfies ExportedHandler<MediaDiscoveryEnv, unknown>;

