Feature: Showing off behave

  Scenario: Run a simple test
    Given we have behave installed
     When we implement 5 tests
     Then behave will test them for us!

  Scenario: Context object introspection
    Given an attribute created in context
      Then I can list the context attributes in another step and check the created attribute
