import { Component, Inject } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';

@Component({
  selector: 'app-cow-dialog',
  template: `
    <h2 mat-dialog-title>{{ data.name }}</h2>
    <mat-dialog-content>
      <p><strong>Age:</strong> {{ data.age }}</p>
      <p><strong>Breed:</strong> {{ data.breed }}</p>
      <p><strong>Notes:</strong> {{ data.notes }}</p>
    </mat-dialog-content>
    <mat-dialog-actions align="end">
      <button mat-button [mat-dialog-close]="true">Close</button>
    </mat-dialog-actions>
  `,
  imports: [
    MatButtonModule, MatDialogModule
  ]
})
export class CowDialogComponent {
  constructor(@Inject(MAT_DIALOG_DATA) public data: any) {}
}