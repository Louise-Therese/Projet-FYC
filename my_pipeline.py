import pandas as pd
import luigi



class TaskA(luigi.Task):
    def run(self):
        
       
        df = pd.read_csv("Auto.csv")
        print("Before reading CSV")
        # Suppression de la colonne B
        df.drop("CITY", axis=1, inplace=True)

        # Enregistrement du DataFrame modifié
        df.to_csv(self.output().path, index=False)
        print("Output path:", self.output().path)

    def output(self):
        # Fichier de sortie
        return luigi.LocalTarget("taskA_output.csv")
        

class TaskB(luigi.Task):
    
    #Création de la dépendance entre la Tâche A et la tâche B
    def requires(self):
        return TaskA()

    def run(self):
        # Chargement DataFrame résultant de la tâche A
        df = pd.read_csv(self.input().path)

        # Création de la colonne Total
        df['Total'] = df['PRICEEACH'] * df['QUANTITYORDERED']

        # Enregistrement du DataFrame modifié
        df.to_csv(self.output().path, index=False)

    def output(self):
        # Fichier de sortie
        return luigi.LocalTarget("taskB_output.csv")

class MyWorkflow(luigi.WrapperTask):
    def requires(self):
        # Liste des tâches nécessaires pour cette étape
        return TaskB()
    
if __name__ == '__main__':
    luigi.build([MyWorkflow()], local_scheduler=True)