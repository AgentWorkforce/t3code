/**
 * AgentRelayTextGeneration — deliberately unsupported.
 *
 * Text generation (commit messages, PR descriptions, branch names, thread
 * titles) needs a structured request/response call into a model. Agent
 * Relay's v1 transport is a raw terminal stream with no such call — there is
 * no way to ask "generate a commit message" and get a parseable answer back
 * without typing it into the live agent's terminal and scraping the reply.
 *
 * Per this repo's "provider-shaped features need a decision, even if the
 * decision is 'not supported here'" rule, this is that decision: every
 * operation fails with a clear `TextGenerationError` instead of the driver
 * omitting the field (which the `ProviderInstance` contract does not allow).
 *
 * @module textGeneration/AgentRelayTextGeneration
 */
import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as TextGeneration from "./TextGeneration.ts";

const unsupported = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail:
        "Agent Relay does not support text generation shortcuts (commit messages, PR content, branch names, thread titles). Pick a different provider for this in Settings.",
    }),
  );

export const makeAgentRelayTextGeneration: Effect.Effect<TextGeneration.TextGeneration["Service"]> =
  Effect.succeed(
    TextGeneration.TextGeneration.of({
      generateCommitMessage: () => unsupported("generateCommitMessage"),
      generatePrContent: () => unsupported("generatePrContent"),
      generateBranchName: () => unsupported("generateBranchName"),
      generateThreadTitle: () => unsupported("generateThreadTitle"),
    }),
  );
