#!/bin/bash
# Usage: ./docker/scraper-cli.sh add <url>
CONTAINER=${COMIC_SCRAPER_CONTAINER:-comic-scraper}
docker exec "$CONTAINER" /usr/local/bin/comic-scraper-cli "$@"