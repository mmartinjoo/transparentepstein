from transparentepstein.classification.classifier.base import Classifier, ClassifierType
from transparentepstein.classification.classifier.regex_classifier import RegexClassifier


def create_classifier(type: ClassifierType) -> Classifier:
    classifiers = {
        ClassifierType.REGEX: RegexClassifier(),
    }
    
    try:
        return classifiers[type]
    except KeyError:
        raise ValueError(f"unknown classifier type: {type}")