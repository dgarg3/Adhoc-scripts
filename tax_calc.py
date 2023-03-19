''' program will have list of tax slabs along with the rates
'''
import sys

tax_rate = [[20550,10],[83550,12],[178150,22],[647851,37]]

'''function to calculate levels or slabs'''
def tax_levels(income):
    re = 0
    for i in range(len(tax_rate)):
        if i == 0:
            re = income
        re -= tax_rate[i][0]
        if re <= 0:
            return i
            break
    return i


'''function to calculate tax'''

def tax_calc(in_income,levels):
    tax = 0
    for i in range(levels):
        upper = tax_rate[i][0]
        tax += min(in_income, upper) * (tax_rate[i][1] / 100)
        remainder = in_income - upper
        in_income = remainder
    return tax

if __name__ == "__main__":
    income = int(sys.argv[1])
    tax_level = tax_levels(income) +1
    print(tax_calc(income,tax_level))









