from transparentepstein.classification.classifier.base import ClassificationLabel, Classifier

class RegexClassifier(Classifier):
    def classify(self, content: str) -> ClassificationLabel:
        if self.match_email(content[:500]):
            return ClassificationLabel.EMAIL
        
        if self.match_court_doc(content[:500]):
            return ClassificationLabel.COURT
        
        if self.match_financial_doc(content[:500]):
            return ClassificationLabel.FINANCIAL
        
        return ClassificationLabel.UNKNOWN
    
    def match_email(self, content: str) -> bool:
        return content.find("From:") != -1 and content.find("To:") != -1 and content.find("Subject:") != -1

    def match_court_doc(self, content: str) -> bool:
        court_document_matchers = [
            "UNITED STATES DISTRICT COURT",
            "UNITED STATES GRAND JURY",
            "United States v. Jeffrey Epstein",
            "United States v. Ghislaine Maxwell",
            "CASE NO",
        ]
        
        contains = [content.find(matcher) != -1 for matcher in court_document_matchers]
        return any(contains)
    
    def match_financial_doc(self, content: str) -> bool:
        financial_document_matchers = [
            "Account Summary",
            "Account Number",
        ]
        
        contains = [content.find(matcher) != -1 for matcher in financial_document_matchers]
        return any(contains)