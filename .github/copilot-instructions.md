# Instructions for Building a Singer Tap/Target

This document provides guidance for implementing a high-quality Singer Tap (or Target) in compliance with the Singer specification and community best practices. Use it in conjunction with GitHub Copilot or your preferred IDE.

---

## 1. Rate Limiting

- Respect API rate limits (e.g., daily quotas or per-second limits).
- For short-term rate limits, detect HTTP 429 or similar errors and implement retries with sleep/delay.
- Use Singer’s built-in rate-limiting utilities where available.


## 2. Memory Efficiency

- Minimize RAM usage by streaming data.
Example: Use generators or iterators instead of loading entire datasets into memory.


## 3. Consistent Date Handling

- Use RFC 3339 format (including time zone offset). UTC (Z) is preferred.
Examples:
Good: 2017-01-01T00:00:00Z, 2017-01-01T00:00:00-05:00
Bad: 2017-01-01 00:00:00
Use pytz for timezone-aware conversions.


## 4. Logging & Exception Handling

- Log every API request (URL + parameters), omitting sensitive info (e.g., API keys).
- Log progress updates (e.g., “Starting stream X”).
- On API errors, log status code and response body.

For fatal errors:
- Log at CRITICAL or FATAL level.
- Exit with non-zero status.
- Omit stack trace for known, user-triggered conditions.
- Include full trace for unexpected exceptions.
- For recoverable errors, implement retries with exponential backoff (e.g., using the backoff library).


## 5. Module Structure

- Organize code into a proper Python module (directory with __init__.py), not a single script file.


## 6. Schema Management

- For static schemas, store them as .json files in a schemas/ directory—not as inline Python dicts.
Prefer explicit schemas:
- Avoid additionalProperties: true or vague typing.
- Use clear field names and types.
- Set additionalProperties: false when schemas must be strict.
- Be cautious when tightening schemas in new versions—it may require a major version bump per semantic versioning.


## 7. JSON Schema Guidelines

- All files under schemas/*.json must follow the JSON Schema standard.
- Any fields named created_time, modified_time, ending in _time or ending in _date must use the date-time format.
- Any fields looks like date-time field, give suggestion to validate the fields should have date-time format.
- Avoid using additionalProperties at the root level. It's allowed in nested fields only.

Example:
{
  "type": "object",
  "properties": {
    "created_time": {
      "type": ["null", "string"],
      "format": "date-time"
    },
    "last_access_time": {
      "type": ["null", "string"],
      "format": "date-time"
    }
  }
}


## 8. Validating Bookmarking

We use the singer.bookmarks module to read from and write to the bookmark state file.
To ensure correctness, always validate the structure of the bookmark state before processing or committing any changes.
- In abstract.py, we use get_bookmark() and write_bookmark() to manage bookmarks for streams.
- The write_bookmark() function overrides the one from the singer module to apply custom behavior.
- Always confirm that the state structure matches the expected format before writing.

Format Example:
{
  "bookmarks": {
    "stream_name": {
      "replication_key": "2024-01-01T00:00:00Z"
    }
  }
}


Optional validation function:
def is_valid_bookmark_state(state):
    return isinstance(state, dict) and \
           "bookmarks" in state and \
           isinstance(state["bookmarks"], dict)


## 9. Code Quality

- Use pylint and aim for zero error-level messages.
- CI pipelines (e.g., CircleCI) should enforce linting.
- Fix or explicitly disable warnings when appropriate.