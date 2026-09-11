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
        def has_subject():
            return content.find("Subject:") != -1 or content.find("Subject Re:") != -1 or content.find("Subject") != -1
        
        def has_from():
            # sometimes From is misread to Fran
            return content.find("From:") != -1 or content.find("Fran:") != -1
            
        
        if has_from() and content.find("To:") != -1 and has_subject():
            return True

        return False        

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