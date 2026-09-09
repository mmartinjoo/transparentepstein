from transparentepstein.classification import create_classifier, ClassifierType

def main():
    classifier = create_classifier(ClassifierType.REGEX)
    label = classifier.classify(content="""
To: jeeyacationti)gmail.com[jeeyacation@gmail.com]; Jeffrey Epsteinueeyacation@gmail.com]
From: Elon Musk
Sent: Sun 11/25/2012 12:36:28 AM
Subject: RE:
Probably just Talulah and me. What day/night will be the wildest party on your
island""")
    
    print(label)