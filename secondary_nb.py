"""
Model exported as python.
Name : secondary_nb
Group : 
With QGIS : 32003
"""
import sys

from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

QgsApplication.setPrefixPath('C:/Program Files/QGIS 3.20.3/apps/qgis', True)
qgs = QgsApplication([], False)
qgs.initQgis()

# Add the path to processing so we can import it next
# sys.path.append(r'C:\OSGeo4W64\apps\qgis\python\plugins')
sys.path.append('C:/Program Files/QGIS 3.20.3/apps/qgis/python/plugins')
# Imports usually should be at the top of a script but this unconventional
# order is necessary here because QGIS has to be initialized first

import processing
from processing.core.Processing import Processing
from processing.script import ScriptUtils



class Secondary_nb(QgsProcessingAlgorithm):

    def processAlgorithm(self, parameters):
        Processing.initialize()
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingFeedback()
        results = {}
        outputs = {}

        # NODEBOUNDARY
        alg_params = {
            'VERBOSE_LOG': False,
            'cluster': parameters['cluster'],
            'demand': parameters['demandpoints'],
            'plotboundary': parameters['landboundary'],
            'FinalBoundary': parameters['Noudeboundary']
        }
        outputs['Nodeboundary'] = processing.run('script:NODEBOUNDARY', alg_params)
        results['Noudeboundary'] = outputs['Nodeboundary']['FinalBoundary']
        return results


obj = Secondary_nb()
res = obj.processAlgorithm({
    'cluster':r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\cluster_output.shp",
    'demandpoints':r"C:\Users\jyothy\Desktop\New folder\geoflask\data_input\demandpoints.shp",
    'landboundary':r"C:\Users\jyothy\Desktop\New folder\geoflask\data_input\landbndry.shp",
    'Noudeboundary':r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\nodeboundary_output.shp"
})
print(res)

