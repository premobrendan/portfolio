import { Component } from '@angular/core';
import { CattleTree } from './cattle-tree/cattle-tree';

@Component({
  selector: 'app-root',
  imports: [CattleTree],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  protected title = 'cattle-tree-app';
}
