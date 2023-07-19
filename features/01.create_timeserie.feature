Feature: Sum a Pair
  It sums a pair of numbers

  Background:
    Given a workspace WORKSPACE_DIR
    And a valid user logged in

  # Esquema do Cenário: adicionando números
  #   Dado a workspace WORKSPACE_DIR
  #   E um número <left>
  #   Quando somar com <right>
  #   Então o resultado é <result>

  Scenario Outline: adding numbers
    Given a number <left>
    When add a number <right>
    Then the sum is <result>

  Examples: Numbers to sum and expected results
    | left  | right | result |
    | 9     | 3     | 12     |
    | 8     | 7     | 15     |
    | 4     | 17    | 21     |
    | 6     | 15    | 21     |
    | 10    | 3     | 13     |

  Scenario: using a data table
    Given a simple silly step
    Then the last step has a final table:
        | Name   | City | Birthday |
        | Alonso | Barcelona | 20/07/1981 |
        | Bred   | London  | 17/05/1980 |
        | Pedro   | Brasilia  | 19/08/1958 |
