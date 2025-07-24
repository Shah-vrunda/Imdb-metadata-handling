require("dotenv").config();
const fetch = require("node-fetch");
const cheerio = require("cheerio");
const sql = require("mssql");

const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME, // Use name if server is not set
  server: process.env.DB_HOST, // Use host if server is not set
  port: Number(process.env.DB_PORT || 1433),
  connectionTimeout: 30000, // 30 seconds
  requestTimeout: 30000,

  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
};

async function processAllExistingImdbLinks() {
  let pool;
  try {
    pool = await sql.connect(config);

    const result = await pool.request().query(`
      SELECT ab.TalentID, ab.IMDBLink
      FROM tblTalentIMDBResume ab
      WHERE ab.TalentID NOT IN (
        SELECT DISTINCT tblm.TalentId FROM tblImdbMetaData tblm
      )
    `);

    const rows = result.recordset;

    for (const row of rows) {
      const { TalentID, IMDBLink } = row;

      try {
        const imdbId = extractImdbId(IMDBLink);
        console.log(`Processing TalentID: ${TalentID}, imdbId: ${imdbId}`);
        if (!imdbId) {
          console.warn(`No valid IMDb ID found for TalentID ${TalentID}`);
          continue;
        }

        const pageHtml = await fetchImdbPage(imdbId);
        const credits = extractFilmographyFromHtml(pageHtml);
        console.log(`Found ${credits.length} credits for TalentID ${TalentID}`);

        for (const credit of credits) {
          console.log(`Inserting credit: ${JSON.stringify(credit)}`);

          await pool
            .request()
            .input("TalentId", sql.Int, TalentID)
            .input("Title", sql.NVarChar(255), credit.title)
            .input("TitleUrl", sql.NVarChar(255), credit.titleUrl)
            .input("Year", sql.NVarChar(10), credit.year)
            .input("Type", sql.NVarChar(50), credit.type)
            .input("Role", sql.NVarChar(255), credit.role).query(`
              INSERT INTO tblImdbMetaData (TalentId, Title, TitleUrl, Year, Type, Role)
              VALUES (@TalentId, @Title, @TitleUrl, @Year, @Type, @Role)
            `);
        }
      } catch (err) {
        console.error(
          `Error processing IMDb for talent ${TalentID}: ${err.message}`
        );
      }
    }
  } catch (err) {
    console.error("Database error:", err);
  } finally {
    if (pool) await pool.close();
  }
}

async function fetchImdbPage(imdbId) {
  const url = `https://www.imdb.com/name/${imdbId}/`;
  const response = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; IMDbProcessor/1.0; +https://yourdomain.com/)",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to fetch IMDb page: ${response.status} ${response.statusText}`
    );
  }
  return await response.text();
}

function extractImdbId(url) {
  if (!url) return null;
  // The '?' makes the trailing slash optional
  const match = url.match(/\/name\/(nm\d+)\/?/);
  return match ? match[1] : null;
}

function extractFilmographyFromHtml(html) {
  const $ = cheerio.load(html);
  const credits = [];
  const nextDataScript = $("#__NEXT_DATA__").html();
  if (!nextDataScript) {
    console.error("__NEXT_DATA__ not found in HTML");
    return credits;
  }
  let imdbJson;
  try {
    imdbJson = JSON.parse(nextDataScript);
  } catch (err) {
    console.error("Failed to parse __NEXT_DATA__ JSON:", err);
    return credits;
  }
  const edges =
    imdbJson?.props?.pageProps?.mainColumnData?.releasedPrimaryCredits?.[0]
      ?.credits?.edges;
  if (!edges || !Array.isArray(edges)) {
    console.warn("No filmography data found in __NEXT_DATA__");
    return credits;
  }
  for (const edge of edges) {
    const node = edge?.node;
    const title = node?.title?.titleText?.text || "";
    const titleUrl = node?.title?.id
      ? `https://www.imdb.com/title/${node.title.id}/`
      : "";
    const year = node?.title?.releaseYear?.year?.toString() || "";
    const type = node?.title?.titleType?.text || "";
    const role = node?.characters?.[0]?.name || "";
    credits.push({ title, titleUrl, year, type, role });
  }
  return credits;
}

(async () => {
  try {
    await processAllExistingImdbLinks();
    console.log("Processing finished.");
    process.exit(0);
  } catch (err) {
    console.error("Unexpected error:", err);
    process.exit(1);
  }
})();
