Feature: Sum a Pair
  It sums a pair of numbers

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
