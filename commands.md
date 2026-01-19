# Maven Commands

## Building & Testing
* `mvn clean com.spotify.fmt:fmt-maven-plugin:format install`

## Version Bumps
* `mvn versions:display-dependency-updates`
* `mvn versions:display-plugin-updates`

## Cutting a Release
* `mvn release:prepare`
* `mvn release:perform`

https://central.sonatype.com/publishing/deployments

### If `release:perform` fails:
* `mvn release:rollback`
* `git fetch origin`

## Updating Github Pages
* `git switch --detach jesque-${LATEST}`
* `mvn clean site site:stage`
* `mvn scm-publish:publish-scm`
* `git switch -`