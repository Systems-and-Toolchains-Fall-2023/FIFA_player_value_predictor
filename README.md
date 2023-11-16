[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/nyeWPUMW)
# FIFA Player Prediction 

## Steps to Run Notebooks
1. Make sure that the "fifa_dataset" folder is at the same directory level as the notebooks for tasks 1,2, and 3. Task 1 and Task 2 are in a seperate notebook due to being part of the same assignment. Task 3 is in its own notebook due to being its own assignment. To run each task, navigate to the respective notebook in your preferred IDE.
2. Make sure the respective libraries and packages are downloaded via pip (or pip3 based on Python version) in order to run through the notebooks. These include PySpark, TensorFlow, PyTorch, Numpy, Pandas, and others (but mainly those 4). If any miscellaneous package errors arise, follow the traceback's instructions on downloading the required package to your Python environment.
3. When going through the Task3 notebook, make sure that there is enough working RAM on your system to run the SparkML portion of the training job. The code currently is configured to allocated 6 GB of overhead memory to account for garbage collection of a lot of the downstream PySpark code and variables.
4. Sequentially run through all the cells in their structured order (most cells run in a few seconds and no hyperparameter tuning cell takes more than 7-8 minutes to execute).


## Model Training Reflection
### Choice of Classifiers/Regressors
#### Multilayer Perceptron (MLP) & Vanilla Feedforward Network: 
These neural networks were chosen due to their ability to model complex, nonlinear relationships in the data. MLP, with its multiple layers, is capable of capturing more intricate patterns compared to a simpler Vanilla network. The choice was driven by the diversity and complexity of the features in the FIFA dataset. Both a simple feedforward and a slightly more complex MLP network were chosen because of their low overhead in terms of computational efficiency as well as using the vanilla feedforward as a baseline to see how more complex neural network architectures perform. 
#### Random Forest (RF): 
Known for its robustness and ability to handle diverse data types, the Random Forest regressor was selected for its ensemble approach, combining multiple decision trees to improve predictive accuracy and control over-fitting. Here, it was imperative to experiment with a tree-based regressor to observe the effects of an ensemble method on the performance of regression.
#### Linear Regression: 
This was chosen as a baseline model due to its simplicity and interpretability. It serves as a point of comparison to evaluate how much more complexity and sophistication the other models bring to the table.

### Impact of Tunable Parameters on Accuracy
#### Learning Rate & Optimizer Type (Adam/SGD): 
For the neural networks (MLP and Vanilla), tuning the learning rate and optimizer type significantly impacted the model's convergence speed and overall performance. A lower learning rate with the Adam optimizer led to a more stable and effective training process. Since learning rate is the obvious parameter to tweak, optimizer type was also chosen to observe the effects of Adam vs SGD, with Adam outperforming SGD almost always.
#### Number of Trees & Max Depth (RF): 
These parameters directly influenced the model's complexity and ability to capture nuances in the data. Increasing the number of trees and depth allowed the model to learn finer details but also posed a risk of overfitting, which was mitigated by the inherent nature of Random Forest.
#### Regularization & Max Iterations (Linear Regression): 
Here, the intuitions for parameter tuning were quite standard and conventional. Regularization helped in preventing overfitting, making the model more generalizable. The number of iterations determined the convergence of the model, with a higher iteration count allowing more room for the model to reach its optimal state.

### Model Comparisons Based on Test-evaluated RMSE
MLP (RMSE: 0.7273, Best Learning Rate: 0.001, Best Optimizer: Adam): This model exhibited the best performance, indicating its ability to effectively capture complex patterns in the data. Its multiple layers and non-linear activation functions provided it with the necessary tools to model the intricate relationships within the FIFA dataset.
### Vanilla Feedforward (RMSE: 0.7311, Best Learning Rate: 0.001, Best Optimizer: Adam): 
This neural net was slightly less effective than the MLP, which can be attributed to its simpler architecture. However, it still performed very well, showcasing the effectiveness of even basic neural network structures on this dataset and their ability to capture intricate patterns in the data.
### Random Forest (RMSE: 0.9169, Best Parameters: {'numTrees': 40, 'maxDepth': 10}): 
This model demonstrated good performance but lagged behind the neural networks ever so slightly. This could be due to its ensemble nature that may not capture the intricacies as effectively as the neural network architectures.
### Linear Regression (RMSE: 1.7483, Best Parameters: {'regParam': 0.001, 'maxIter': 100}): 
As expected, this model had the highest RMSE, reflecting its relative limitations in handling complex and non-linear relationships compared to more sophisticated models. It still performed quite well on a baseline showing how well the dataset was preprocessed and normalized for training purposes.

### Computational Constraints
The project initially pursued aggressive hyperparameter tuning, but this approach was soon stymied by the computational limitations of a local machine. Lengthy training times and memory constraints highlighted the need for a balance between model complexity and computational feasibility, leading to a more judicious selection and tuning of hyperparameters.


### FIFA Dataset Schema Metadata
| Feature                | Data Type | Nullable | Description                                                 | Constraints                        |
|-----------------------------|-----------|----------|-------------------------------------------------------------|-----------------------------------|
| sofifa_id                   | integer   | true     | Unique identifier for the player in the SoFIFA database.    | > 0                               |
| player_url                  | string    | true     | URL link to the player's profile on the SoFIFA website.     | Valid URL                              |
| short_name                  | string    | true     | Shortened version of the player's name.                     | None                              |
| long_name                   | string    | true     | Full version of the player's name.                          | None                              |
| player_positions            | string    | true     | Positions the player can play in, separated by commas.      | None                              |
| overall                     | integer   | true     | Player's overall rating.                                    | 1-100                             |
| potential                   | integer   | true     | Player's potential rating.                                  | 1-100                             |
| value_eur                   | double    | true     | Market value of the player in Euros.                        | >= 0                              |
| wage_eur                    | double    | true     | Player's weekly wage in Euros.                              | >= 0                              |
| age                         | integer   | true     | Player's age.                                               | None            |
| dob                         | date      | true     | Player's date of birth.                                     | Must be a past date               |
| height_cm                   | integer   | true     | Player's height in centimeters.                             | None                         |
| weight_kg                   | integer   | true     | Player's weight in kilograms.                               | None                           |
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
| pace to goalkeeping_speed     | various   | true     | Fields (from 38 to 72) that are specific skill ratings for the player, indicating their proficiency and skill in various aspects of FIFA. | 1-100                   |
| ls to gk                      | string    | true     | Fields (from 73 to 95) that indicate the player's rating when positioned in various positions on the field. | Formatted as "XX+Y" (e.g., "65+2")                   |
| player_face_url               | string    | true     | URL link to the player's face image.                      | Valid URL                                |
| club_logo_url                 | string    | true     | URL link to the club's logo.                              | Valid URL                                |
| club_flag_url                 | string    | true     | URL link to the club's flag or emblem.                    | Valid URL                                |
| nation_logo_url               | string    | true     | URL link to the national team's logo.                     | Valid URL                                |
| nation_flag_url               | string    | true     | URL link to the national team's flag.                     | Valid URL                                |
| year                          | integer   | true     | Year of the dataset.                                      | 2015-2022                                                  |
| unique_id                     | long      | false    | Unique identifier for each row in the dataset.            | > 0 and must be unique                               |

