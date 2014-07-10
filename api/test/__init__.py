import sys
import os

ROOT = os.path.join([os.path.dirname(os.path.abspath(__file__)), '..'])

print ROOT

sys.path = [ROOT] + sys.path


