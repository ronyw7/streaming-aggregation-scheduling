import unittest
from scheduler import Operator, AdaptiveScheduler   

class BudgetCalculationTest(unittest.TestCase):
    def test_example(self):
        source = Operator("load", 10, None, 100, 8, operator_type='source')
        source.output_size = 100

        # Transform operator
        # T_1 = 12, E_1 = 6, doubles output size
        transform = Operator("transform", 12, 100, 200, 6, operator_type='transform')
        
        # Inference operator
        # T_2 = 2, E_2 = 4
        inference = Operator("inference", 2, 200, 200, 4, operator_type='inference')
        
        operators = [source, transform, inference]
        initial_budget = 1000
        scheduler = AdaptiveScheduler(operators, source, initial_budget)
        
        scheduler.update_budget()
        new_budget = scheduler.budget
        
        # According to example:
        # P_1 = 12/6 * 1 = 2
        # P_2 = 2/4 * 2 = 1
        # P = 2 + 1 = 3
        # Budget increment = source_output_size/P = 100/3 = 33.33
        expected_budget_increment = 100/3
        expected_new_budget = initial_budget + expected_budget_increment
        
        print(f"Expected: {initial_budget} + {expected_budget_increment} = {expected_new_budget}")
        print(f"Actual: {new_budget}")
        self.assertAlmostEqual(new_budget, expected_new_budget, places=2)


if __name__ == '__main__':
    unittest.main()