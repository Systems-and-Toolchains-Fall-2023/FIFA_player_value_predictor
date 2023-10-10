[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/nyeWPUMW)

### FIFA Dataset Schema
| Column Name                   | Data Type | Nullable |
|-------------------------------|-----------|----------|
| sofifa_id                     | integer   | true     |
| player_url                    | string    | true     |
| short_name                    | string    | true     |
| long_name                     | string    | true     |
| player_positions              | string    | true     |
| overall                       | integer   | true     |
| potential                     | integer   | true     |
| value_eur                     | double    | true     |
| wage_eur                      | double    | true     |
| age                           | integer   | true     |
| dob                           | date      | true     |
| height_cm                     | integer   | true     |
| weight_kg                     | integer   | true     |
| club_team_id                  | double    | true     |
| club_name                     | string    | true     |
| league_name                   | string    | true     |
| league_level                  | integer   | true     |
| club_position                 | string    | true     |
| club_jersey_number            | integer   | true     |
| club_loaned_from              | string    | true     |
| club_joined                   | date      | true     |
| club_contract_valid_until     | integer   | true     |
| nationality_id                | integer   | true     |
| nationality_name              | string    | true     |
| nation_team_id                | double    | true     |
| nation_position               | string    | true     |
| nation_jersey_number          | integer   | true     |
| preferred_foot                | string    | true     |
| weak_foot                     | integer   | true     |
| skill_moves                   | integer   | true     |
| international_reputation      | integer   | true     |
| work_rate                     | string    | true     |
| body_type                     | string    | true     |
| real_face                     | string    | true     |
| release_clause_eur            | string    | true     |
| player_tags                   | string    | true     |
| player_traits                 | string    | true     |
| pace                          | integer   | true     |
| shooting                      | integer   | true     |
| passing                       | integer   | true     |
| dribbling                     | integer   | true     |
| defending                     | integer   | true     |
| physic                        | integer   | true     |
| attacking_crossing            | integer   | true     |
| attacking_finishing           | integer   | true     |
| attacking_heading_accuracy    | integer   | true     |
| attacking_short_passing       | integer   | true     |
| attacking_volleys             | integer   | true     |
| skill_dribbling               | integer   | true     |
| skill_curve                   | integer   | true     |
| skill_fk_accuracy             | integer   | true     |
| skill_long_passing            | integer   | true     |
| skill_ball_control            | integer   | true     |
| movement_acceleration         | integer   | true     |
| movement_sprint_speed         | integer   | true     |
| movement_agility              | integer   | true     |
| movement_reactions            | integer   | true     |
| movement_balance              | integer   | true     |
| power_shot_power              | integer   | true     |
| power_jumping                 | integer   | true     |
| power_stamina                 | integer   | true     |
| power_strength                | integer   | true     |
| power_long_shots              | integer   | true     |
| mentality_aggression          | integer   | true     |
| mentality_interceptions       | integer   | true     |
| mentality_positioning         | integer   | true     |
| mentality_vision              | integer   | true     |
| mentality_penalties           | integer   | true     |
| mentality_composure           | string    | true     |
| defending_marking_awareness   | integer   | true     |
| defending_standing_tackle     | integer   | true     |
| defending_sliding_tackle      | integer   | true     |
| goalkeeping_diving            | integer   | true     |
| goalkeeping_handling          | integer   | true     |
| goalkeeping_kicking           | integer   | true     |
| goalkeeping_positioning       | integer   | true     |
| goalkeeping_reflexes          | integer   | true     |
| goalkeeping_speed             | integer   | true     |
| ls                            | string    | true     |
| st                            | string    | true     |
| rs                            | string    | true     |
| lw                            | string    | true     |
| lf                            | string    | true     |
| cf                            | string    | true     |
| rf                            | string    | true     |
| rw                            | string    | true     |
| lam                           | string    | true     |
| cam                           | string    | true     |
| ram                           | string    | true     |
| lm                            | string    | true     |
| lcm                           | string    | true     |
| cm                            | string    | true     |
| rcm                           | string    | true     |
| rm                            | string    | true     |
| lwb                           | string    | true     |
| ldm                           | string    | true     |
| cdm                           | string    | true     |
| rdm                           | string    | true     |
| rwb                           | string    | true     |
| lb                            | string    | true     |
| lcb                           | string    | true     |
| cb                            | string    | true     |
| rcb                           | string    | true     |
| rb                            | string    | true     |
| gk                            | string    | true     |
| player_face_url               | string    | true     |
| club_logo_url                 | string    | true     |
| club_flag_url                 | string    | true     |
| nation_logo_url               | string    | true     |
| nation_flag_url               | string    | true     |
| year                          | integer   | true     |
| unique_id                     | integer   | true     |

### Feature Descriptions
sofifa_id: Unique identifier for the player in the SoFIFA database.
player_url: URL link to the player's profile on the SoFIFA website.
short_name: Shortened version of the player's name.
long_name: Full version of the player's name.
player_positions: Positions the player can play in, often separated by commas.
overall: Player's overall rating.
potential: Player's potential rating, indicating their future performance capabilities.
value_eur: Market value of the player in Euros.
wage_eur: Player's weekly wage in Euros.
age: Player's age.
dob: Player's date of birth.
height_cm: Player's height in centimeters.
weight_kg: Player's weight in kilograms.
club_team_id: Unique identifier for the player's club team.
club_name: Name of the club the player belongs to.
league_name: Name of the league in which the player's club competes.
league_level: Level or tier of the league.
club_position: Position the player usually occupies in the club.
club_jersey_number: Player's jersey number in the club.
club_loaned_from: Name of the club the player is loaned from, if applicable.
club_joined: Date when the player joined the current club.
club_contract_valid_until: Year until which the player's contract with the club is valid.
nationality_id: Unique identifier for the player's nationality.
nationality_name: Name of the player's nationality.
nation_team_id: Unique identifier for the player's national team.
nation_position: Position the player usually occupies in the national team.
nation_jersey_number: Player's jersey number in the national team.
preferred_foot: Player's preferred foot (Right or Left).
weak_foot: Rating (usually out of 5) of the player's non-preferred foot skills.
skill_moves: Rating (usually out of 5) indicating the player's skill moves capability.
international_reputation: Rating (usually out of 5) of the player's international reputation.
work_rate: Player's work rate, usually defined for both attacking and defending (e.g., "High/High").
body_type: Categorical description of the player's body type.
real_face: Indicates if the player has a real face scan in the game.
release_clause_eur: Release clause value in Euros, if applicable.
player_tags: Tags associated with the player, separated by commas.
player_traits: Specific traits associated with the player, separated by commas.
pace to goalkeeping_speed: These fields (from 38 to 72) are specific skill ratings for the player, usually out of 100, indicating their proficiency in various aspects of the game.
ls to gk: These fields (from 73 to 95) indicate the player's rating when positioned in various positions on the field.
player_face_url: URL link to the player's face image.
club_logo_url: URL link to the club's logo.
club_flag_url: URL link to the club's flag or emblem.
nation_logo_url: URL link to the national team's logo.
nation_flag_url: URL link to the national team's flag.
year: Year of the dataset.
unique_id: Unique identifier for each row in the dataset.