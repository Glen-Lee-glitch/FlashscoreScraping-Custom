<h1 align="center">
  <img src=".github/Logo.svg" alt="logo" width=150px>
</h1>

Flashscore is a popular website providing live scores, statistics, and news across various sports. However, it lacks an
official API for developers to access its data. This is where FlashscoreScraping comes in.

This project serves users seeking reliable sports results data. Sports enthusiasts can use the scraper data to
to track their favorite teams, analyze past results, and predict future outcomes. Additionally, researchers,
students, and educators can utilize the data for academic purposes.

<img src=".github/FlashscoreScraping.gif" alt="logo" width=600px>

## Getting Started

To get started with FlashscoreScraping, follow these steps:

1. Clone the project:

   ```bash
   git clone https://github.com/gustavofariaa/FlashscoreScraping.git
   ```

1. Navigate to the project directory:

   ```bash
   cd FlashscoreScraping
   ```

1. Install dependencies:

   ```bash
   npm install
   ```

1. Run the scraper:

   Once everything is installed, you can run the scraper using the following command:

   ```bash
   npm run start
   ```

## Available Command-Line Parameters

The scraper allows you to specify the country, league, output file type (JSON or CSV), and whether to run in headless mode.

| Parameter     | Default Value | Description                                              |
| :------------ | :-----------: | :------------------------------------------------------- |
| `country`     |       -       | The country for which results are to be crawled.         |
| `league`      |       -       | The league for which results are to be crawled.          |
| `fileType`    |    `json`     | The format of the output file (`JSON` or `CSV`).         |
| `no-headless` |    `false`    | If set, the scraper will run with a graphical interface. |

### Example commands

- Scrape Brazilian Serie A 2023 results and save as a `JSON` file:

  ```bash
  npm run start country=brazil league=serie-a-2023 fileType=json
  ```

- Scrape English Premier League 2022-2023 results with a graphical interface and save as a `CSV` file:

  ```bash
  npm run start country=england league=premier-league-2022-2023 no-headless fileType=csv
  ```

## Data Example

When scraping match data, you’ll receive detailed information about each match, such as the match date, teams, scores, and statistics. Below is an example of what the data might look like in `JSON` format:

### JSON Format

```json
{
  "Gd4glas0": {
    "stage": "MINEIRO - ROUND 7",
    "date": "09.02.2025 16:00",
    "status": "FINISHED",
    "home": {
      "name": "Cruzeiro",
      "id": "lCWrxmg5"
    },
    "away": {
      "name": "Atletico-MG",
      "id": "WbSJHDh5"
    },
    "result": {
      "home": "0",
      "away": "2"
    },
    "statistics": [
      {
        "category": "Ball Possession",
        "homeValue": "42%",
        "awayValue": "58%"
      }
    ],
    "odds": {
      "over-under": [
        {
          "handicap": "2.5",
          "average": {
            "over": "1.85",
            "under": "1.95"
          },
          "bookmakers": [
            {
              "bookmaker": "1xBet",
              "over": "1.83",
              "under": "1.97"
            },
            {
              "bookmaker": "Bet365",
              "over": "1.87",
              "under": "1.93"
            }
          ]
        },
        {
          "handicap": "3.5",
          "average": {
            "over": "2.75",
            "under": "1.44"
          },
          "bookmakers": [
            {
              "bookmaker": "1xBet",
              "over": "2.70",
              "under": "1.46"
            }
          ]
        }
      ]
    }
  }
}
```

## Data Breakdown

1. Match Date

   - `stage`: The name of the competition and round (e.g., "MINEIRO - ROUND 7").
   - `date`: The date and time the match took place.
   - `status`: The match status (e.g., FINISHED).

1. Team

   An object representing the team, containing:

   - `name`: The team's name.
   - `id`: The team's unique identifier (used in Flashscore URLs).

1. Result

   The match result, including:

   - `home`: The home team's score.
   - `away`: The away team's score.
   - `regulationTime`: The result of the match in regular time, if applicable (null if not).
   - `penalties`: The penalty score, if applicable (null if not).

1. Statistics

   An array of match statistics, each with the following structure:

   - `category`: The name of the statistic (e.g., "Expected Goals (xG)").
   - `homeValue`: The statistic value for the home team.
   - `awayValue`: The statistic value for the away team.

1. Odds

   Betting odds data for the match, organized by market type:

   - `over-under`: Over/Under betting odds (Full Time). An array of handicap lines, each containing:
     - `handicap`: The goal line (e.g., "2.5", "3.5")
     - `average`: Average odds across all bookmakers for this handicap
       - `over`: Average odds for over the handicap
       - `under`: Average odds for under the handicap
     - `bookmakers`: Array of individual bookmaker odds with the same structure
       - `bookmaker`: Name of the bookmaker (e.g., "1xBet", "Bet365")
       - `over`: Odds for over
       - `under`: Odds for under

   Note: Odds may be `null` if not available for a particular match. Additional market types (1X2, handicap, etc.) can be added in the future.

---

If you encounter any issues or have suggestions for improvements, feel free
to [open an issue](https://github.com/gustavofariaa/FlashscoreScraping/issues).
