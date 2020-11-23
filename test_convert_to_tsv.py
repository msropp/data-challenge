from unittest import TestCase
import create_parsable_tsv


class test_create_parsable_tsv(TestCase):
    def test_get_file_contents(self):
        filename = "./data/data.tsv"
        contents = create_parsable_tsv.get_file_contents(filename)

        expected_length = 1008
        actual_length = len(contents)

        self.assertEqual(expected_length, actual_length)

    def test_clean_line_line1(self):
        filename = "./data/data.tsv"
        input = create_parsable_tsv.get_file_contents(filename)[1]

        expectedOutput = "1\tAddison\tMarks\t196296\tornare.lectus@et.edu"
        actualOutput = create_parsable_tsv.clean_line(input)

        self.assertEqual(expectedOutput, actualOutput)

    def test_clean_line_line613(self):
        filename = "./data/data.tsv"
        input = create_parsable_tsv.get_file_contents(filename)[618]

        expectedOutput = "613\t'\\t'\t'\\t'\t104969\tdictum@Suspendisse.net"
        actualOutput = create_parsable_tsv.clean_line(input)

        self.assertEqual(expectedOutput, actualOutput)

    def test_fix_line_item_issues(self):
        item = "29\tAdena\tHobbs\tBosley\t656184\tac.ipsum.Phasellus@ut.net"
        expectedOutput = "29\tAdena\tHobbs\t656184\tac.ipsum.phasellus@ut.net"
        actualOutput = create_parsable_tsv.fix_line_item_issues(item)

        self.assertEqual(expectedOutput, actualOutput)

    def test_create_parsable_file_contents(self):
        filename = "./data/data.tsv"
        input = create_parsable_tsv.get_file_contents(filename)[:3]

        expectedOutput = ['id\tfirst_name\tlast_name\taccount_number\temail\n', '1\tAddison\tMarks\t196296\tornare.lectus@et.edu\n', '2\tDakota\tGarza\t409025\tscelerisque@praesentluctus.edu']
        actualOutput = create_parsable_tsv.create_parsable_file_contents(input)

        self.assertEqual(expectedOutput, actualOutput)
