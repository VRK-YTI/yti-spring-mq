Library for asyn mq-based distibuted import-task executor

Check out latest tag:
```
git pull
git tag
git checkout <latest-tag>
```

Install for local development with `gradlew publishToMavenLocal` on Windows or `./gradlew publishToMavenLocal` on Linux/Mac OS.

Configuration is done at  `application.properties`-file or profile specific one. There exist different eviction policies, so select one  which works for your case. Just add following lines to control  cache behaviour.

```
# Status mq config
spring.cache.cache-names: Terminology
spring.cache.caffeine.spec: maximumSize=100, expireAfterAccess=30s

```
More information can be found in `https://www.baeldung.com/java-caching-caffeine`
