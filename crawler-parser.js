const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;


async function scrapeSearchResults(browser, keyword, locality, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const formattedLocality = locality.replace(" ", "+");

        const page = await browser.newPage();
        try {
            const url = `https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=${formattedKeyword}&location=${formattedLocality}&original_referer=`;
    
            await page.goto(url);

            console.log(`Successfully fetched: ${url}`);

            const divCards = await page.$$("div[class='base-search-card__info']");

            for (const divCard of divCards) {

                const nameElement = await divCard.$("h4[class='base-search-card__subtitle']");
                const name = await page.evaluate(element => element.textContent, nameElement);

                const jobTitleElement = await divCard.$("h3[class='base-search-card__title']");
                const jobTitle = await page.evaluate(element => element.textContent, jobTitleElement);

                const parentElement = await page.evaluateHandle(element => element.parentElement, divCard);

                const aTag = await parentElement.$("a");
                const link = await page.evaluate(element => element.getAttribute("href"), aTag);

                const jobLocationElement = await divCard.$("span[class='job-search-card__location']");
                const jobLocation = await page.evaluate(element => element.textContent, jobLocationElement);

                const searchData = {
                    name: name.trim(),
                    job_title: jobTitle.trim(),
                    url: link.trim(),
                    location: jobLocation.trim()
                };

                console.log(searchData);

            }

            success = true;

        } catch (err) {
            console.log(`Error: ${err}, tries left ${retries - tries}`);
            tries++;

        } finally {
            await page.close();
        } 
    }
}

async function startCrawl(keyword, locality, location, retries) {

    const browser = await puppeteer.launch();

    await scrapeSearchResults(browser, keyword, locality, location, retries);

    await browser.close();
}


async function main() {
    const keywords = ["software engineer"];
    const concurrencyLimit = 5;
    const pages = 1;
    const location = "us";
    const locality = "United States";
    const retries = 3;
    const aggregateFiles = [];

    for (const keyword of keywords) {
        console.log("Crawl starting");
        console.time("startCrawl");
        await startCrawl(keyword, pages, locality, location, retries);
        console.timeEnd("startCrawl");
        console.log("Crawl complete");
    }
}


main();