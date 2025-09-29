package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class Yep {

    @Test
    void name() {

        ObjectValue.builder()
                .putFields("id", LongValue.strict())
                .putFields("type", StringValue.strict())
                .putFields("actor", ObjectValue.strict(Map.of("id", LongValue.strict())))
                .putFields("repo", ObjectValue.strict(Map.of("id", LongValue.strict())))
//                .putFields("payload", ObjectValue.strict(Map.of("action", StringValue.strict())))
                .putFields("created_at", InstantValue.strict())
                .putFields("org", ObjectValue.strict(Map.of("id", LongValue.strict())))
                .build();
    }

    /*
    {
  "id": "2489651057",
  "type": "WatchEvent",
  "actor": {
    "id": 6894991,
    "login": "SametSisartenep",
    "gravatar_id": "",
    "url": "https://api.github.com/users/SametSisartenep",
    "avatar_url": "https://avatars.githubusercontent.com/u/6894991?"
  },
  "repo": {
    "id": 2871998,
    "name": "visionmedia/debug",
    "url": "https://api.github.com/repos/visionmedia/debug"
  },
  "payload": {
    "action": "started"
  },
  "public": true,
  "created_at": "2015-01-01T15:00:03Z",
  "org": {
    "id": 9285252,
    "login": "visionmedia",
    "gravatar_id": "",
    "url": "https://api.github.com/orgs/visionmedia",
    "avatar_url": "https://avatars.githubusercontent.com/u/9285252?"
  }
}
     */

    /*
    {
  "id": "45193146662",
  "type": "WatchEvent",
  "actor": {
    "id": 62409730,
    "login": "parnexcodes",
    "display_login": "parnexcodes",
    "gravatar_id": "",
    "url": "https://api.github.com/users/parnexcodes",
    "avatar_url": "https://avatars.githubusercontent.com/u/62409730?"
  },
  "repo": {
    "id": 900627378,
    "name": "yuaotian/go-cursor-help",
    "url": "https://api.github.com/repos/yuaotian/go-cursor-help"
  },
  "payload": {
    "action": "started"
  },
  "public": true,
  "created_at": "2025-01-01T15:00:00Z"
}
     */
}
