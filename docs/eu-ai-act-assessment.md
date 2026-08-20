# EU AI Act assessment

**Assessment date:** 19 August 2026
**Scope:** the source tree and documented runtime behaviour of the AProDB beta

## Current conclusion

On its current functionality, AProDB is a database and deterministic compute engine, not an AI system or general-purpose AI model under Regulation (EU) 2024/1689. It does not infer how to generate predictions, content, recommendations, or decisions; it contains no trained model, chatbot, or generative-AI runtime integration. Exact vector scoring, scheduling, and optional GPU execution do not by themselves change that conclusion.

The use of OpenAI Codex as an assistive development tool does not turn the resulting database into an AI system and does not require a change to the software licences. The European Commission's Article 50 guidance also lists source code among the outputs outside the provider-side machine-readable marking obligation. Andrea Provenzali has performed human selection, review, and editorial control of the repository and remains responsible for publication.

For provenance beyond the minimum legal requirement, the repository contains [AI_ASSISTANCE.md](../AI_ASSISTANCE.md). No AI service is named as an author or copyright holder.

## Reassessment triggers

Reassess before a release that adds any of the following:

- a trained or general-purpose AI model bundled with or served by AProDB;
- inference that generates predictions, recommendations, decisions, or content;
- a chatbot or another system that interacts directly with natural persons;
- generation or manipulation of audio, image, video, or text;
- use in a high-risk context or a prohibited practice covered by the AI Act.

## Official sources

- [Regulation (EU) 2024/1689, especially Articles 2, 3, 50 and 113](https://eur-lex.europa.eu/eli/reg/2024/1689/oj?locale=en)
- [European Commission guidelines on the AI-system definition](https://digital-strategy.ec.europa.eu/en/library/commission-publishes-guidelines-ai-system-definition-facilitate-first-ai-acts-rules-application)
- [European Commission Article 50 transparency guidance](https://digital-strategy.ec.europa.eu/en/library/guidelines-transparency-obligations-providers-and-deployers-ai-systems)
- [European Commission Article 50 questions and answers](https://digital-strategy.ec.europa.eu/en/faqs/transparency-obligations-under-article-50-ai-act)

This is a project-level technical assessment, not legal advice. It must be revisited when functionality, intended use, or authoritative guidance changes.