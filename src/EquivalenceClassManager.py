from typing import List,Dict
from SPJGExpression import SPJGExpression,column

class EquivalenceClassManager:
    #列等价类，3.1.1 并查集
    def __init__(self,all_cols,Eqpredicates):
        self.fa= {}
        self.height= {}
        for col in all_cols:
            self.fa[col]=col
            self.height[col]=0

        for eqpredicate in Eqpredicates:
            self.merge(eqpredicate[0],eqpredicate[1])

    def get(self,col):
        if col==self.fa[col]:
            return self.fa[col]
        else:
            self.fa[col]=self.get(self.fa[col])
            return self.fa[col]

    def is_equivalent(self,col1,col2):
        try:
            c1 = self.get(col1)
            c2 = self.get(col2)
            return c1==c2
        except:
            return False

    def merge(self,col1,col2):
        c1=self.get(col1)
        c2=self.get(col2)
        if self.height[c1]<self.height[c2]:
            self.fa[c1]=c2
            self.height[c2]+=self.height[c1]
        else:
            self.fa[c2]=c1
            self.height[c1]+=self.height[c2]

    def get_all_equivalences(self):
        classes={}
        for col in self.fa:
            rt=self.get(col)
            if rt not in classes:
                classes[rt]=set()
            classes[rt].add(col)
        return list(classes.values())

    def get_all_eq_cols(self,col):
        try:
            eq_classes=self.get_all_equivalences()
            for eq_class in eq_classes:
                if col in eq_class:
                    return eq_class
            return []
        except:
            return []
