# Usage: .\docker\scraper-cli.ps1 add <url>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Arguments
)

$container = $env:COMIC_SCRAPER_CONTAINER
if (-not $container) {
    $container = "comic-scraper"
}

docker exec $container /usr/local/bin/comic-scraper-cli @Arguments