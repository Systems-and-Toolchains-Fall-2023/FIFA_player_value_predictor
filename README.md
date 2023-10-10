[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/nyeWPUMW)

### FIFA Dataset Schema Metadata
| Feature                | Data Type | Nullable | Description                                                 | Constraints                        |
|-----------------------------|-----------|----------|-------------------------------------------------------------|-----------------------------------|
| sofifa_id                   | integer   | true     | Unique identifier for the player in the SoFIFA database.    | > 0                               |
| player_url                  | string    | true     | URL link to the player's profile on the SoFIFA website.     | None                              |
| short_name                  | string    | true     | Shortened version of the player's name.                     | None                              |
| long_name                   | string    | true     | Full version of the player's name.                          | None                              |
| player_positions            | string    | true     | Positions the player can play in, separated by commas.      | None                              |
| overall                     | integer   | true     | Player's overall rating.                                    | 1-100                             |
| potential                   | integer   | true     | Player's potential rating.                                  | 1-100                             |
| value_eur                   | double    | true     | Market value of the player in Euros.                        | >= 0                              |
| wage_eur                    | double    | true     | Player's weekly wage in Euros.                              | >= 0                              |
| age                         | integer   | true     | Player's age.                                               | 15-50 (approximately)             |
| dob                         | date      | true     | Player's date of birth.                                     | Must be a past date               |
| height_cm                   | integer   | true     | Player's height in centimeters.                             | 150-210                           |
| weight_kg                   | integer   | true     | Player's weight in kilograms.                               | 50-100                            |
| club_team_id                | double    | true     | Unique identifier for the player's club team.               | >= 0                              |
| club_name                   | string    | true     | Name of the club the player belongs to.                     | None                              |
| league_name                 | string    | true     | Name of the league in which the player's club competes.     | None                              |
| league_level                | integer   | true     | Level or tier of the league.                                | > 0                               |
| club_position               | string    | true     | Position the player usually occupies in the club.           | None                              |
| club_jersey_number          | integer   | true     | Player's jersey number in the club.                         | 1-99                              |
| club_loaned_from            | string    | true     | Name of the club the player is loaned from, if applicable.  | None                              |
| club_joined                 | date      | true     | Date when the player joined the current club.               | Must be a past date               |
| club_contract_valid_until   | integer   | true     | Year until which the player's contract is valid.            | >= 'year' column                  |
| nationality_id              | integer   | true     | Unique identifier for the player's nationality.             | > 0                               |
| nationality_name            | string    | true     | Name of the player's nationality.                           | None                              |
| nation_team_id              | double    | true     | Unique identifier for the player's national team.           | >= 0                              |
| nation_position             | string    | true     | Position the player usually occupies in the national team.  | None                              |
| nation_jersey_number        | integer   | true     | Player's jersey number in the national team.               | 1-99                              |
| preferred_foot              | string    | true     | Player's preferred foot.                                    | "Right" or "Left"                 |
| weak_foot                   | integer   | true     | Rating of the player's non-preferred foot skills.           | 1-5                               |
| skill_moves                 | integer   | true     | Player's skill moves capability rating.                     | 1-5                               |
| international_reputation    | integer   | true     | Rating of the player's international reputation.            | 1-5                               |
| work_rate                   | string    | true     | Player's work rate, for both attacking and defending.       | e.g., "High/High", "Medium/High"  |
| body_type                     | string    | true     | Categorical description of the player's body type.        | None                                                 |
| real_face                     | string    | true     | Indicates if the player has a real face scan in the game. | "Yes" or "No"                                        |
| release_clause_eur            | string    | true     | Release clause value in Euros, if applicable.             | Should represent a number, or "None"                 |
| player_tags                   | string    | true     | Tags associated with the player, separated by commas.     | None                                                 |
| player_traits                 | string    | true     | Specific traits associated with the player, separated by commas. | None                                      |
| pace to goalkeeping_speed     | various   | true     | These fields (from 38 to 72) are specific skill ratings for the player, usually out of 100, indicating their proficiency in various aspects of the game. | 1-100                   |
| ls to gk                      | string    | true     | These fields (from 73 to 95) indicate the player's rating when positioned in various positions on the field. | Formatted as "XX+Y" (e.g., "65+2")                   |
| player_face_url               | string    | true     | URL link to the player's face image.                      | Should be a valid URL                                |
| club_logo_url                 | string    | true     | URL link to the club's logo.                              | Should be a valid URL                                |
| club_flag_url                 | string    | true     | URL link to the club's flag or emblem.                    | Should be a valid URL                                |
| nation_logo_url               | string    | true     | URL link to the national team's logo.                     | Should be a valid URL                                |
| nation_flag_url               | string    | true     | URL link to the national team's flag.                     | Should be a valid URL                                |
| year                          | integer   | true     | Year of the dataset.                                      | > 0                                                  |
| unique_id                     | long      | false    | Unique identifier for each row in the dataset.            | > 0 and must be unique                               |

