from __future__ import annotations

from typing import Annotated

from pydantic import Field, model_validator

from .thesis_shared_slice import _ContractModel


FootnoteIndex = Annotated[int, Field(ge=1)]


class LetterParagraph(_ContractModel):
    text: str = Field(..., min_length=1)


class LetterSection(_ContractModel):
    title: str = Field(..., min_length=1)
    body: list[LetterParagraph] = Field(..., min_length=1)
    footnote_indexes: list[FootnoteIndex] = Field(...)


class MethodologyCitation(_ContractModel):
    footnote_index: int = Field(..., ge=1)
    citation_text: str = Field(..., min_length=1)
    methodology_path: str = Field(..., min_length=1)


class LpLetter(_ContractModel):
    firm_name: str = Field(..., min_length=1)
    letter_date: str = Field(..., min_length=1)
    recipient_salutation: str = Field(..., min_length=1)
    performance_attribution: LetterSection = Field(...)
    position_commentary: LetterSection = Field(...)
    drawdown_framing: LetterSection = Field(...)
    outlook: LetterSection = Field(...)
    citations: list[MethodologyCitation] = Field(...)

    @model_validator(mode="after")
    def _validate_footnotes(self) -> "LpLetter":
        citation_indexes = [citation.footnote_index for citation in self.citations]
        duplicate_indexes = {
            index for index in citation_indexes if citation_indexes.count(index) > 1
        }
        if duplicate_indexes:
            duplicates = ", ".join(str(index) for index in sorted(duplicate_indexes))
            raise ValueError(f"citation footnote_index values must be unique: {duplicates}")

        citation_index_set = set(citation_indexes)
        missing_indexes = sorted(
            {
                index
                for section in (
                    self.performance_attribution,
                    self.position_commentary,
                    self.drawdown_framing,
                    self.outlook,
                )
                for index in section.footnote_indexes
                if index not in citation_index_set
            }
        )
        if missing_indexes:
            missing = ", ".join(str(index) for index in missing_indexes)
            raise ValueError(f"section footnote_indexes missing citations: {missing}")

        return self
